// native/book/commit.cpp — the Merkle-treap of commit.hpp, and its C ABI.
#include "commit.hpp"
#include "keccak.hpp"
#include <cstring>
#include <new>

namespace rollcommit {

static const u8 ZERO32[32] = {0};
constexpr u32 T_EMPTY = 0xFFFFFFFFu, T_TOMB = 0xFFFFFFFEu;

// ------------------------------------------------------------------ hashing
/// restingLeaf: 0x00 ‖ abi.encode(bytes32 hash, address user, bool buy, uint256 price, uint256 remaining, uint256 seq)
void leaf_hash(const Entry& e, u8 out[32]) noexcept {
  u8 b[1 + 6 * 32]; std::memset(b, 0, sizeof b);
  b[0] = 0x00;
  std::memcpy(b + 1, e.hash, 32);                        // bytes32, left-aligned (it fills the word)
  std::memcpy(b + 1 + 32 + 12, e.user, 20);              // address, right-aligned in its word
  b[1 + 2 * 32 + 31] = e.buy ? 1 : 0;                    // bool, a word of 0/1
  std::memcpy(b + 1 + 3 * 32, e.price, 32);              // uint256
  std::memcpy(b + 1 + 4 * 32, e.remaining, 32);          // uint256
  u64 s = e.seq; for (int i = 0; i < 8; ++i) { b[1 + 5 * 32 + 31 - i] = (u8)s; s >>= 8; }   // uint256 seq, big-endian
  keccak::hash256(b, sizeof b, out);
}
/// the first 8 bytes, big-endian, of keccak256(side ‖ price ‖ seq)
u64 priority_of(bool buy, const u8 price[32], u64 seq) noexcept {
  u8 b[41]; b[0] = buy ? 0 : 1; std::memcpy(b + 1, price, 32);
  for (int i = 0; i < 8; ++i) b[33 + i] = (u8)(seq >> (8 * (7 - i)));
  u8 h[32]; keccak::hash256(b, sizeof b, h);
  u64 p = 0; for (int i = 0; i < 8; ++i) p = (p << 8) | h[i];
  return p;
}
/// 0x03 ‖ leaf ‖ left ‖ right. `out` may alias an input: the input is copied before the hash is written.
static void node_hash(const u8 leaf[32], const u8 l[32], const u8 r[32], u8 out[32]) noexcept {
  u8 b[97]; b[0] = 0x03;
  std::memcpy(b + 1, leaf, 32); std::memcpy(b + 33, l, 32); std::memcpy(b + 65, r, 32);
  keccak::hash256(b, sizeof b, out);
}

bool verify(const Proof& p, const u8 root[32]) noexcept {
  u8 h[32]; node_hash(p.leaf, p.left, p.right, h);
  for (const ProofStep& s : p.path) {
    if (s.dir == 0) node_hash(s.leaf, h, s.sibling, h);
    else if (s.dir == 1) node_hash(s.leaf, s.sibling, h, h);
    else return false;
  }
  return std::memcmp(h, root, 32) == 0;
}
size_t proof_bytes(const Proof& p) noexcept { return 96 + 65 * p.path.size(); }
void proof_write(const Proof& p, u8* out) noexcept {
  std::memcpy(out, p.leaf, 32); std::memcpy(out + 32, p.left, 32); std::memcpy(out + 64, p.right, 32);
  u8* q = out + 96;
  for (const ProofStep& s : p.path) { q[0] = s.dir; std::memcpy(q + 1, s.leaf, 32); std::memcpy(q + 33, s.sibling, 32); q += 65; }
}
bool proof_read(const u8* in, size_t n, Proof& out) {
  if (n < 96 || (n - 96) % 65) return false;
  std::memcpy(out.leaf, in, 32); std::memcpy(out.left, in + 32, 32); std::memcpy(out.right, in + 64, 32);
  out.path.clear(); out.path.resize((n - 96) / 65);
  const u8* q = in + 96;
  for (ProofStep& s : out.path) { s.dir = q[0]; std::memcpy(s.leaf, q + 1, 32); std::memcpy(s.sibling, q + 33, 32); q += 65; }
  return true;
}

// ------------------------------------------------------------------ the tree
CommitTree::CommitTree(size_t hint) {
  nodes_.reserve(hint);
  size_t cap = 16; while (cap < hint * 2) cap <<= 1;
  tab_rebuild(cap);
}

u32 CommitTree::alloc() {
  if (free_head_ != NIL) { u32 s = free_head_; free_head_ = nodes_[s].left; return s; }
  nodes_.emplace_back();
  return (u32)(nodes_.size() - 1);
}
void CommitTree::release(u32 s) noexcept { nodes_[s].live = 0; nodes_[s].left = free_head_; free_head_ = s; }

int CommitTree::cmp(u32 a, u8 side, const u8* price, u64 seq, const u8* hash) const noexcept {
  const Node& n = nodes_[a];
  if (n.side != side) return n.side < side ? -1 : 1;
  int c = std::memcmp(n.price, price, 32); if (c) return c < 0 ? -1 : 1;
  if (n.seq != seq) return n.seq < seq ? -1 : 1;
  c = std::memcmp(n.hash, hash, 32); return c < 0 ? -1 : c > 0 ? 1 : 0;
}
int CommitTree::cmp(u32 a, u32 b) const noexcept { const Node& n = nodes_[b]; return cmp(a, n.side, n.price, n.seq, n.hash); }
bool CommitTree::higher(u32 a, u32 b) const noexcept {
  const Node& x = nodes_[a]; const Node& y = nodes_[b];
  if (x.prio != y.prio) return x.prio > y.prio;
  return cmp(a, b) < 0;                       // a priority tie (same side, price and seq): the smaller key is the parent
}
void CommitTree::pull(u32 n) noexcept {
  Node& x = nodes_[n];
  node_hash(x.leaf, x.left == NIL ? ZERO32 : nodes_[x.left].node_hash, x.right == NIL ? ZERO32 : nodes_[x.right].node_hash, x.node_hash);
  ++keccaks;
}
void CommitTree::entry_of(const Node& x, Entry& e) const noexcept {
  std::memcpy(e.hash, x.hash, 32); std::memcpy(e.user, x.user, 20); e.buy = x.side == 0;
  std::memcpy(e.price, x.price, 32); std::memcpy(e.remaining, x.remaining, 32); e.seq = x.seq;
}

/// insert n below t by key, then rotate it up while its priority beats its parent's; every node whose
/// children changed is re-pulled on the way back up
u32 CommitTree::ins(u32 t, u32 n) noexcept {
  if (t == NIL) { pull(n); return n; }
  if (cmp(n, t) < 0) {
    u32 l = ins(nodes_[t].left, n); nodes_[t].left = l;
    if (higher(l, t)) { nodes_[t].left = nodes_[l].right; nodes_[l].right = t; pull(t); t = l; }   // rotate right
  } else {
    u32 r = ins(nodes_[t].right, n); nodes_[t].right = r;
    if (higher(r, t)) { nodes_[t].right = nodes_[r].left; nodes_[r].left = t; pull(t); t = r; }    // rotate left
  }
  pull(t); return t;
}
/// join two treaps where every key of a is below every key of b
u32 CommitTree::merge(u32 a, u32 b) noexcept {
  if (a == NIL) return b;
  if (b == NIL) return a;
  if (higher(a, b)) { u32 r = merge(nodes_[a].right, b); nodes_[a].right = r; pull(a); return a; }
  u32 l = merge(a, nodes_[b].left); nodes_[b].left = l; pull(b); return b;
}
/// unlink node n from the subtree t: its children are merged in its place
u32 CommitTree::del(u32 t, u32 n) noexcept {
  if (t == NIL) return NIL;
  if (t == n) return merge(nodes_[t].left, nodes_[t].right);
  if (cmp(n, t) < 0) { u32 l = del(nodes_[t].left, n); nodes_[t].left = l; }
  else { u32 r = del(nodes_[t].right, n); nodes_[t].right = r; }
  pull(t); return t;
}
/// re-pull the path from the root down to n (after n's leaf changed)
void CommitTree::repull(u32 t, u32 n) noexcept {
  if (t == NIL) return;
  if (t != n) { if (cmp(n, t) < 0) repull(nodes_[t].left, n); else repull(nodes_[t].right, n); }
  pull(t);
}
void CommitTree::inorder(u32 t, std::vector<Entry>& out) const {
  if (t == NIL) return;
  inorder(nodes_[t].left, out);
  Entry e; entry_of(nodes_[t], e); out.push_back(e);
  inorder(nodes_[t].right, out);
}

bool CommitTree::insert(const Entry& e) {
  if (find(e.hash) != NIL) return false;
  u32 s = alloc();
  {
    Node& x = nodes_[s];
    std::memcpy(x.hash, e.hash, 32); std::memcpy(x.user, e.user, 20);
    std::memcpy(x.price, e.price, 32); std::memcpy(x.remaining, e.remaining, 32);
    x.side = e.buy ? 0 : 1; x.live = 1; x.seq = e.seq; x.left = x.right = NIL;
    x.prio = priority_of(e.buy, e.price, e.seq);
    leaf_hash(e, x.leaf);
    keccaks += 2;
  }
  root_ = ins(root_, s);
  tab_insert(e.hash, s);
  ++count_;
  return true;
}
bool CommitTree::remove(const u8 hash[32]) noexcept {
  u32 s = find(hash); if (s == NIL) return false;
  root_ = del(root_, s);
  tab_erase(hash); release(s); --count_;
  return true;
}
bool CommitTree::set_remaining(const u8 hash[32], const u8 remaining[32]) noexcept {
  u32 s = find(hash); if (s == NIL) return false;
  Node& x = nodes_[s];
  std::memcpy(x.remaining, remaining, 32);
  Entry e; entry_of(x, e); leaf_hash(e, x.leaf); ++keccaks;
  repull(root_, s);
  return true;
}
void CommitTree::root(u8 out[32]) const noexcept {
  if (root_ == NIL) std::memset(out, 0, 32); else std::memcpy(out, nodes_[root_].node_hash, 32);
}
bool CommitTree::get(const u8 hash[32], Entry& out) const noexcept {
  u32 s = find(hash); if (s == NIL) return false;
  entry_of(nodes_[s], out); return true;
}
bool CommitTree::proof(const u8 hash[32], Proof& out) const {
  u32 s = find(hash); if (s == NIL) return false;
  std::vector<ProofStep> down;              // ancestors, top-down
  u32 t = root_;
  while (t != s) {
    if (t == NIL) return false;             // cannot happen: s is in the tree
    const Node& x = nodes_[t]; ProofStep st; std::memcpy(st.leaf, x.leaf, 32);
    if (cmp(s, t) < 0) { st.dir = 0; std::memcpy(st.sibling, x.right == NIL ? ZERO32 : nodes_[x.right].node_hash, 32); t = x.left; }
    else { st.dir = 1; std::memcpy(st.sibling, x.left == NIL ? ZERO32 : nodes_[x.left].node_hash, 32); t = x.right; }
    down.push_back(st);
  }
  const Node& n = nodes_[s];
  std::memcpy(out.leaf, n.leaf, 32);
  std::memcpy(out.left, n.left == NIL ? ZERO32 : nodes_[n.left].node_hash, 32);
  std::memcpy(out.right, n.right == NIL ? ZERO32 : nodes_[n.right].node_hash, 32);
  out.path.assign(down.rbegin(), down.rend());
  return true;
}
void CommitTree::entries(std::vector<Entry>& out) const { out.clear(); out.reserve(count_); inorder(root_, out); }

// ------------------------------------------------------------------ hash → slot
static inline u64 h64(const u8* h) noexcept { u64 v = 0; for (int i = 0; i < 8; ++i) v = (v << 8) | h[i]; return v; }
void CommitTree::tab_rebuild(size_t cap) {
  std::vector<u32> old; old.swap(tab_);
  tab_.assign(cap, T_EMPTY); tmask_ = cap - 1; tcount_ = 0; ttomb_ = 0;
  for (u32 s : old) if (s != T_EMPTY && s != T_TOMB) tab_insert(nodes_[s].hash, s);
}
u32 CommitTree::find(const u8 hash[32]) const noexcept {
  size_t i = h64(hash) & tmask_;
  for (;;) {
    u32 v = tab_[i];
    if (v == T_EMPTY) return NIL;
    if (v != T_TOMB && std::memcmp(nodes_[v].hash, hash, 32) == 0) return v;
    i = (i + 1) & tmask_;
  }
}
void CommitTree::tab_insert(const u8 hash[32], u32 slot) {
  if ((tcount_ + ttomb_ + 1) * 2 > tab_.size())                        // past half full, tombstones included:
    tab_rebuild(tcount_ * 4 > tab_.size() ? tab_.size() * 2 : tab_.size());   // grow, or just purge the tombstones
  size_t i = h64(hash) & tmask_;
  while (tab_[i] != T_EMPTY && tab_[i] != T_TOMB) i = (i + 1) & tmask_;   // the key is known to be absent (insert() looked)
  if (tab_[i] == T_TOMB) --ttomb_;
  tab_[i] = slot; ++tcount_;
}
void CommitTree::tab_erase(const u8 hash[32]) noexcept {
  size_t i = h64(hash) & tmask_;
  for (;;) {
    u32 v = tab_[i];
    if (v == T_EMPTY) return;
    if (v != T_TOMB && std::memcmp(nodes_[v].hash, hash, 32) == 0) { tab_[i] = T_TOMB; --tcount_; ++ttomb_; return; }
    i = (i + 1) & tmask_;
  }
}

}  // namespace rollcommit

// ------------------------------------------------------------------ C ABI
using rollcommit::CommitTree; using rollcommit::Entry; using rollcommit::Proof;
extern "C" {
void* commit_new(void) { return new (std::nothrow) CommitTree(1024); }
void commit_free(void* t) { delete static_cast<CommitTree*>(t); }
int32_t commit_insert(void* t, const uint8_t* hash32, const uint8_t* user20, int32_t buy, const uint8_t* price32, const uint8_t* remaining32, uint64_t seq) {
  if (!t || !hash32 || !user20 || !price32 || !remaining32) return -1;
  Entry e; std::memcpy(e.hash, hash32, 32); std::memcpy(e.user, user20, 20); e.buy = buy != 0;
  std::memcpy(e.price, price32, 32); std::memcpy(e.remaining, remaining32, 32); e.seq = seq;
  return static_cast<CommitTree*>(t)->insert(e) ? 1 : 0;
}
int32_t commit_remove(void* t, const uint8_t* hash32) { if (!t || !hash32) return -1; return static_cast<CommitTree*>(t)->remove(hash32) ? 1 : 0; }
int32_t commit_set_remaining(void* t, const uint8_t* hash32, const uint8_t* remaining32) { if (!t || !hash32 || !remaining32) return -1; return static_cast<CommitTree*>(t)->set_remaining(hash32, remaining32) ? 1 : 0; }
int32_t commit_root(void* t, uint8_t* out32) { if (!t || !out32) return -1; static_cast<CommitTree*>(t)->root(out32); return 0; }
uint64_t commit_size(void* t) { return t ? (uint64_t)static_cast<CommitTree*>(t)->size() : 0; }
int32_t commit_proof(void* t, const uint8_t* hash32, uint8_t* out, int32_t cap) {
  if (!t || !hash32) return -1;
  Proof p; if (!static_cast<CommitTree*>(t)->proof(hash32, p)) return -1;
  size_t n = rollcommit::proof_bytes(p);
  if (!out || cap < 0 || (size_t)cap < n) return -2;
  rollcommit::proof_write(p, out); return (int32_t)n;
}
int32_t commit_verify(const uint8_t* proof, int32_t len, const uint8_t* root32) {
  if (!proof || !root32 || len < 0) return 0;
  Proof p; if (!rollcommit::proof_read(proof, (size_t)len, p)) return 0;
  return rollcommit::verify(p, root32) ? 1 : 0;
}
}
