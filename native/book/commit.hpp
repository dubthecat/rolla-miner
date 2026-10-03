// native/book/commit.hpp — the incremental state commitment: a Merkle-treap over the resting book.
//
// docs/L3-MINERS.md §6.6 / §9.5a: `bookHashOf` rebuilds a Merkle root over EVERY resting order at each epoch
// boundary — 48 µs a leaf in JavaScript, 2.4 s for a 50,000-order book, paid by the sequencer and by every
// miner, and the RunPod runs lag by exactly that. This is the replacement: a tree maintained alongside the
// book, O(log n) per insert, fill and cancel, a root readable in O(1), and an inclusion proof for any
// resting order so a challenge can prove one order against the commitment.
//
// ------------------------------------------------------------------ the structure
//
// A treap: a binary search tree over the key (side, price, seq) — side 0 = bid, 1 = ask; price a 32-byte
// big-endian unsigned; seq the book's sequence number — that is at the same time a max-heap over a priority
// derived from the key alone: priority = the first 8 bytes (big-endian uint64) of keccak256(side ‖ price ‖
// seq). Because both the ordering and the priorities are functions of the KEY SET, so is the shape of the
// tree: any history of inserts and removes that arrives at the same set of resting orders arrives at the
// same tree, hence the same root. A canonical commitment, like a sorted Merkle list, but O(log n) to
// maintain — the expected depth of a treap is ~2·ln(n), 21 levels at 50,000 orders.
//
// A shard holds several books and each numbers its own sequence, so (side, price, seq) is not unique across
// a shard (merkle.js's bookHashOf has the same note): the order hash is the final tiebreaker of the key
// order, and of a priority tie. With unique (side, price, seq) the order is exactly ascending (side, price,
// seq) and the hash never decides anything.
//
// ------------------------------------------------------------------ the hashing
//
//   leaf(n)  = keccak256(0x00 ‖ abi.encode(bytes32 hash, address user, bool buy, uint256 price,
//                                          uint256 remaining, uint256 seq))        == merkle.js restingLeaf
//   node(n)  = keccak256(0x03 ‖ leaf(n) ‖ node(n.left) ‖ node(n.right))            32 zero bytes for no child
//   root     = node(top), or 32 zero bytes for an empty tree
//
// 0x00 and 0x03 are merkle.js's LEAF and TREAP tags: a leaf (193 bytes under the hash) can never be re-read
// as a node (97 bytes), nor either as one of the batch tree's 0x01/0x02 nodes. Every node hash is cached,
// so an operation rehashes the path from the touched node to the root and nothing else.
//
// A proof of one resting order is its leaf, its two child hashes, and for each ancestor that ancestor's leaf,
// the hash of the sibling subtree and which side the proven subtree hung on; verify() recomputes the root
// from those alone. engine/l3/miner/commit.js is the JavaScript twin with the same bytes; commit_diff.mjs
// checks the two agree root for root and proof for proof over 100,000 random operations.
#pragma once
#include <cstdint>
#include <cstddef>
#include <vector>

namespace rollcommit {

using u8 = uint8_t; using u32 = uint32_t; using u64 = uint64_t;
constexpr u32 NIL = 0xFFFFFFFFu;

/// what a resting order contributes: exactly restingLeaf()'s fields. price and remaining are 32-byte big-endian.
struct Entry { u8 hash[32]; u8 user[20]; bool buy; u8 price[32]; u8 remaining[32]; u64 seq; };

/// one ancestor on the way up: its leaf, the hash of the subtree on the other side, and which side we came from
struct ProofStep { u8 leaf[32]; u8 sibling[32]; u8 dir; };   // dir 0: the proven subtree was this ancestor's LEFT child; 1: RIGHT
/// the proof of one order: its own leaf and children, then the path from its parent up to the root
struct Proof { u8 leaf[32]; u8 left[32]; u8 right[32]; std::vector<ProofStep> path; };

void leaf_hash(const Entry& e, u8 out[32]) noexcept;
u64  priority_of(bool buy, const u8 price[32], u64 seq) noexcept;
bool verify(const Proof& p, const u8 root[32]) noexcept;
/// the proof's wire form (the C ABI's and commit.js's encodeCommitProof): leaf ‖ left ‖ right ‖ n × (dir ‖ leaf ‖ sibling)
size_t proof_bytes(const Proof& p) noexcept;
void   proof_write(const Proof& p, u8* out) noexcept;
bool   proof_read(const u8* in, size_t n, Proof& out);

class CommitTree {
 public:
  explicit CommitTree(size_t hint = 1024);
  bool insert(const Entry& e);                                              // false: this hash is already in the tree
  bool remove(const u8 hash[32]) noexcept;                                  // false: unknown hash
  bool set_remaining(const u8 hash[32], const u8 remaining[32]) noexcept;   // a partial fill; false: unknown hash
  void root(u8 out[32]) const noexcept;                                     // O(1): the cached top hash, or zeros
  size_t size() const noexcept { return count_; }
  bool get(const u8 hash[32], Entry& out) const noexcept;
  bool proof(const u8 hash[32], Proof& out) const;
  /// every entry in key order (side, price, seq, hash) — the canonical list the tree commits to
  void entries(std::vector<Entry>& out) const;
  u64 keccaks = 0;        // keccak256 calls so far, for the bench

 private:
  struct Node {
    u8 node_hash[32], leaf[32], hash[32], price[32], remaining[32], user[20];
    u8 side, live;
    u64 seq, prio;
    u32 left, right;      // `left` doubles as the free-list link of a released slot
  };
  int  cmp(u32 a, u8 side, const u8* price, u64 seq, const u8* hash) const noexcept;   // key order
  int  cmp(u32 a, u32 b) const noexcept;
  bool higher(u32 a, u32 b) const noexcept;     // heap order: priority, then the key as the tiebreaker
  void pull(u32 n) noexcept;                    // recompute node_hash from the cached leaf and the children
  void entry_of(const Node& x, Entry& e) const noexcept;
  u32  ins(u32 t, u32 n) noexcept;
  u32  del(u32 t, u32 n) noexcept;
  u32  merge(u32 a, u32 b) noexcept;
  void repull(u32 t, u32 n) noexcept;
  void inorder(u32 t, std::vector<Entry>& out) const;
  u32  alloc();
  void release(u32 s) noexcept;
  // hash → slot: open addressing with linear probing and tombstones. Order hashes are already uniform, so
  // their first 8 bytes are the index; the full 32 bytes are compared.
  u32  find(const u8 hash[32]) const noexcept;
  void tab_insert(const u8 hash[32], u32 slot);
  void tab_erase(const u8 hash[32]) noexcept;
  void tab_rebuild(size_t cap);

  std::vector<Node> nodes_;
  u32 free_head_ = NIL, root_ = NIL;
  std::vector<u32> tab_;
  size_t tcount_ = 0, ttomb_ = 0, tmask_ = 0, count_ = 0;
};

}  // namespace rollcommit

// ---------------------------------------------------------------------------------------------------
// C ABI, exported by libbook.so next to the book's. Byte buffers only: hashes and 256-bit values are
// big-endian byte arrays, addresses 20 bytes, so a caller never has to agree on a struct layout.
extern "C" {
void*    commit_new(void);
void     commit_free(void* t);
/// 1 inserted, 0 an order with this hash is already in the tree, -1 bad handle or null argument
int32_t  commit_insert(void* t, const uint8_t* hash32, const uint8_t* user20, int32_t buy, const uint8_t* price32, const uint8_t* remaining32, uint64_t seq);
int32_t  commit_remove(void* t, const uint8_t* hash32);                                       // 1 removed, 0 unknown, -1 bad
int32_t  commit_set_remaining(void* t, const uint8_t* hash32, const uint8_t* remaining32);   // 1 updated, 0 unknown, -1 bad
int32_t  commit_root(void* t, uint8_t* out32);                                               // 0, or -1 on a bad handle
uint64_t commit_size(void* t);
/// the proof of one resting order in its wire form; returns the byte count, -1 unknown hash, -2 `cap` too small
int32_t  commit_proof(void* t, const uint8_t* hash32, uint8_t* out, int32_t cap);
int32_t  commit_verify(const uint8_t* proof, int32_t len, const uint8_t* root32);           // 1 valid, 0 not
}
