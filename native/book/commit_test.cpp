// native/book/commit_test.cpp — unit tests for keccak.hpp and the Merkle-treap of commit.hpp.
//   ./commit_test        — prints one line per test, exits non-zero on the first failure
// The pinned vectors (a restingLeaf, a one-node root, a priority) were computed by engine/l3/miner/merkle.js;
// commit.test.mjs pins the same constants on the JavaScript side, so the two suites agree on the bytes.
#include "commit.hpp"
#include "keccak.hpp"
#include <algorithm>
#include <cstdio>
#include <cstring>
#include <string>
#include <vector>

using namespace rollcommit;
static int failures = 0, checks = 0;
static const char* current = "";
#define CHECK(cond) do { ++checks; if (!(cond)) { ++failures; printf("  FAIL %s:%d  %s\n", current, __LINE__, #cond); } } while (0)
#define TEST(name) current = name; printf("· %s\n", name);

static std::string hexof(const u8* p, size_t n) { static const char* d = "0123456789abcdef"; std::string s; for (size_t i = 0; i < n; ++i) { s += d[p[i] >> 4]; s += d[p[i] & 15]; } return s; }
static void unhex(const char* s, u8* out, size_t n) { for (size_t i = 0; i < n; ++i) { unsigned v; sscanf(s + 2 * i, "%2x", &v); out[i] = (u8)v; } }
static std::string k256(const std::string& m) { u8 h[32]; keccak::hash256((const u8*)m.data(), m.size(), h); return hexof(h, 32); }
static void be32(u64 v, u8 out[32]) { std::memset(out, 0, 32); for (int i = 0; i < 8; ++i) { out[31 - i] = (u8)v; v >>= 8; } }

// a deterministic byte source for the random fixtures (xorshift64*)
struct Rng { u64 s; explicit Rng(u64 seed) : s(seed ? seed : 1) {} u64 next() { s ^= s >> 12; s ^= s << 25; s ^= s >> 27; return s * 2685821657736338717ull; } void fill(u8* p, size_t n) { for (size_t i = 0; i < n; ++i) p[i] = (u8)(next() >> 56); } };
static Entry random_entry(Rng& r, u64 seq) {
  Entry e; r.fill(e.hash, 32); r.fill(e.user, 20); e.buy = (r.next() & 1) != 0;
  be32(1 + r.next() % 10000, e.price); be32(1 + r.next() % 1000000, e.remaining); e.seq = seq;
  return e;
}
static std::string root_of(const std::vector<Entry>& es) { CommitTree t; for (const Entry& e : es) t.insert(e); u8 r[32]; t.root(r); return hexof(r, 32); }
static std::string root_hex(const CommitTree& t) { u8 r[32]; t.root(r); return hexof(r, 32); }

int main() {
  {
    TEST("keccak-256: the published vectors, a block boundary, and a two-block message");
    CHECK(k256("") == "c5d2460186f7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a470");
    CHECK(k256("abc") == "4e03657aea45a94fc7d47ba826c8d667c0d1e6e33a64a036ec44f58fa12d6c45");
    CHECK(k256(std::string(135, 'a')) == "34367dc248bbd832f4e3e69dfaac2f92638bd0bbd18f2912ba4ef454919cf446");   // pad byte shares the last byte of the block
    CHECK(k256(std::string(136, 'a')) == "a6c4d403279fe3e0af03729caada8374b5ca54d8065329a3ebcaeb4b60aa386e");   // exactly one block, padding needs a second
    CHECK(k256(std::string(200, 'a')) == "96ea54061def936c4be90b518992fdc6f12f535068a256229aca54267b4d084d");
    // incremental == one shot, whatever the chunking
    std::string m(500, 'x'); for (size_t i = 0; i < m.size(); ++i) m[i] = (char)(i * 7);
    u8 a[32], b[32]; keccak::hash256((const u8*)m.data(), m.size(), a);
    keccak::Hasher h; for (size_t i = 0; i < m.size(); i += 13) h.update((const u8*)m.data() + i, std::min<size_t>(13, m.size() - i)); h.finish(b);
    CHECK(std::memcmp(a, b, 32) == 0);
  }
  Entry pinned; unhex("abababababababababababababababababababababababababababababababab", pinned.hash, 32);
  unhex("1111111111111111111111111111111111111111", pinned.user, 20); pinned.buy = true;
  be32(550000000000000000ull, pinned.price); be32(3000000000000000000ull, pinned.remaining); pinned.seq = 7;
  {
    TEST("the leaf is merkle.js's restingLeaf, the one-node root and the priority are the pinned words");
    u8 leaf[32]; leaf_hash(pinned, leaf);
    CHECK(hexof(leaf, 32) == "da812cb1a3ca38a5c6ae55087ae14e7734a4278ba5a0688e07d79e223bdc516a");
    CommitTree t; CHECK(root_hex(t) == std::string(64, '0')); CHECK(t.size() == 0);
    CHECK(t.insert(pinned)); CHECK(t.size() == 1);
    CHECK(root_hex(t) == "4bbea421ef8c5a4519eee2d3ebd2cc228cebe798d4b3f6249502421c06be83d5");
    CHECK(priority_of(true, pinned.price, 7) == 0xce320c10acce9837ull);
    Proof p; CHECK(t.proof(pinned.hash, p)); CHECK(p.path.empty());
    CHECK(hexof(p.leaf, 32) == hexof(leaf, 32)); CHECK(hexof(p.left, 32) == std::string(64, '0'));
    u8 r[32]; t.root(r); CHECK(verify(p, r));
    CHECK(t.remove(pinned.hash)); CHECK(root_hex(t) == std::string(64, '0')); CHECK(t.size() == 0);
  }
  {
    TEST("determinism: the same set in three insertion orders is the same tree; a different set is not");
    Rng r(42); std::vector<Entry> es; for (u64 i = 1; i <= 2000; ++i) es.push_back(random_entry(r, i));
    std::string a = root_of(es);
    std::vector<Entry> rev(es.rbegin(), es.rend()); CHECK(root_of(rev) == a);
    std::vector<Entry> sh = es; for (size_t i = sh.size(); i > 1; --i) std::swap(sh[i - 1], sh[r.next() % i]); CHECK(root_of(sh) == a);
    std::vector<Entry> less(es.begin(), es.end() - 1); CHECK(root_of(less) != a);
    // and the in-order walk is the canonical (side, price, seq) list
    CommitTree t; for (const Entry& e : sh) t.insert(e);
    std::vector<Entry> walk; t.entries(walk); CHECK(walk.size() == es.size());
    bool sorted = true;
    for (size_t i = 1; i < walk.size(); ++i) {
      int sa = walk[i - 1].buy ? 0 : 1, sb = walk[i].buy ? 0 : 1;
      int c = sa != sb ? (sa < sb ? -1 : 1) : std::memcmp(walk[i - 1].price, walk[i].price, 32);
      if (c == 0) c = walk[i - 1].seq < walk[i].seq ? -1 : walk[i - 1].seq > walk[i].seq ? 1 : 0;
      if (c > 0) sorted = false;
    }
    CHECK(sorted);
  }
  {
    TEST("remove and set_remaining agree with a tree rebuilt from scratch, through 5,000 random operations");
    Rng r(7); CommitTree t; std::vector<Entry> model; u64 seq = 0; bool ok = true;
    for (int i = 0; i < 5000 && ok; ++i) {
      u64 d = r.next() % 100;
      if (!model.empty() && d < 25) {
        size_t k = r.next() % model.size(); ok = ok && t.remove(model[k].hash); model.erase(model.begin() + (long)k);
      } else if (!model.empty() && d < 50) {
        size_t k = r.next() % model.size(); be32(1 + r.next() % 1000000, model[k].remaining); ok = ok && t.set_remaining(model[k].hash, model[k].remaining);
      } else {
        Entry e = random_entry(r, ++seq); ok = ok && t.insert(e); model.push_back(e);
      }
      if (i % 250 == 249) {
        std::vector<Entry> sh = model; for (size_t j = sh.size(); j > 1; --j) std::swap(sh[j - 1], sh[r.next() % j]);
        if (root_of(sh) != root_hex(t) || t.size() != model.size()) { ok = false; printf("  diverged at op %d\n", i); }
      }
    }
    CHECK(ok); CHECK(t.size() == model.size()); CHECK(t.size() > 1000);
  }
  {
    TEST("proofs verify against the root; any tampering fails; the wire form round-trips");
    Rng r(11); CommitTree t; std::vector<Entry> es; for (u64 i = 1; i <= 500; ++i) { es.push_back(random_entry(r, i)); t.insert(es.back()); }
    u8 root[32]; t.root(root); bool all = true; size_t maxdepth = 0;
    for (const Entry& e : es) {
      Proof p; if (!t.proof(e.hash, p) || !verify(p, root)) all = false;
      maxdepth = std::max(maxdepth, p.path.size());
      u8 leaf[32]; leaf_hash(e, leaf); if (std::memcmp(leaf, p.leaf, 32)) all = false;
      std::vector<u8> w(proof_bytes(p)); proof_write(p, w.data()); Proof q; if (!proof_read(w.data(), w.size(), q) || !verify(q, root)) all = false;
    }
    CHECK(all); CHECK(maxdepth >= 8 && maxdepth <= 40);
    Proof p; CHECK(t.proof(es[123].hash, p)); CHECK(verify(p, root)); CHECK(!p.path.empty());
    { Proof q = p; q.leaf[5] ^= 1; CHECK(!verify(q, root)); }
    { Proof q = p; q.right[0] ^= 1; CHECK(!verify(q, root)); }
    { Proof q = p; q.path[0].sibling[31] ^= 1; CHECK(!verify(q, root)); }
    { Proof q = p; q.path[0].dir ^= 1; CHECK(!verify(q, root)); }
    { Proof q = p; q.path[0].dir = 2; CHECK(!verify(q, root)); }
    { Proof q = p; q.path.pop_back(); CHECK(!verify(q, root)); }
    { Proof q = p; q.path.push_back(q.path.back()); CHECK(!verify(q, root)); }
    { u8 r2[32]; std::memcpy(r2, root, 32); r2[0] ^= 1; CHECK(!verify(p, r2)); }
    { std::vector<u8> w(proof_bytes(p)); proof_write(p, w.data()); Proof q; CHECK(!proof_read(w.data(), w.size() - 1, q)); CHECK(!proof_read(w.data(), 95, q)); }
    Proof none; CHECK(!t.proof(pinned.hash, none));
  }
  {
    TEST("a partial fill changes the root, and filling it back restores it");
    Rng r(5); CommitTree t; std::vector<Entry> es; for (u64 i = 1; i <= 300; ++i) { es.push_back(random_entry(r, i)); t.insert(es.back()); }
    std::string before = root_hex(t);
    u8 half[32]; be32(1, half);
    CHECK(t.set_remaining(es[77].hash, half)); std::string after = root_hex(t); CHECK(after != before);
    Entry g; CHECK(t.get(es[77].hash, g)); CHECK(std::memcmp(g.remaining, half, 32) == 0);
    CHECK(t.set_remaining(es[77].hash, es[77].remaining)); CHECK(root_hex(t) == before);
    CHECK(!t.set_remaining(pinned.hash, half)); CHECK(root_hex(t) == before);
    CHECK(!t.remove(pinned.hash)); CHECK(t.size() == 300);
  }
  {
    TEST("a duplicate hash is refused; a duplicate (side, price, seq) under another hash is ordered by hash, canonically");
    Rng r(9); CommitTree t; Entry a = random_entry(r, 1); CHECK(t.insert(a)); CHECK(!t.insert(a)); CHECK(t.size() == 1);
    Entry b = a; r.fill(b.hash, 32);                     // same side, price and seq, a different order (another book of the shard)
    Entry c = a; r.fill(c.hash, 32);
    CommitTree x, y;
    x.insert(a); x.insert(b); x.insert(c);
    y.insert(c); y.insert(a); y.insert(b);
    CHECK(x.size() == 3 && y.size() == 3); CHECK(root_hex(x) == root_hex(y));
    std::vector<Entry> w; x.entries(w); bool byhash = std::memcmp(w[0].hash, w[1].hash, 32) < 0 && std::memcmp(w[1].hash, w[2].hash, 32) < 0; CHECK(byhash);
    Proof p; u8 root[32]; x.root(root); CHECK(x.proof(b.hash, p) && verify(p, root));
  }
  {
    TEST("slots are reused: 100k insert/remove pairs through a tree that never holds more than two orders");
    Rng r(3); CommitTree t; Entry keep = random_entry(r, 1); t.insert(keep); std::string one = root_hex(t); bool ok = true;
    for (u64 i = 2; i <= 100001; ++i) { Entry e = random_entry(r, i); ok = ok && t.insert(e) && t.remove(e.hash); }
    CHECK(ok); CHECK(t.size() == 1); CHECK(root_hex(t) == one);
    Proof p; u8 root[32]; t.root(root); CHECK(t.proof(keep.hash, p) && verify(p, root) && p.path.empty());
  }
  {
    TEST("the C ABI: new / insert / set_remaining / root / proof / verify / remove / free");
    void* h = commit_new(); CHECK(h != nullptr);
    u8 root[32]; CHECK(commit_root(h, root) == 0); CHECK(hexof(root, 32) == std::string(64, '0'));
    CHECK(commit_insert(h, pinned.hash, pinned.user, 1, pinned.price, pinned.remaining, 7) == 1);
    CHECK(commit_insert(h, pinned.hash, pinned.user, 1, pinned.price, pinned.remaining, 7) == 0);
    CHECK(commit_size(h) == 1);
    CHECK(commit_root(h, root) == 0); CHECK(hexof(root, 32) == "4bbea421ef8c5a4519eee2d3ebd2cc228cebe798d4b3f6249502421c06be83d5");
    Rng r(1); for (u64 i = 2; i <= 50; ++i) { Entry e = random_entry(r, i); CHECK(commit_insert(h, e.hash, e.user, e.buy, e.price, e.remaining, e.seq) == 1); }
    CHECK(commit_size(h) == 50);
    u8 buf[4096]; int32_t n = commit_proof(h, pinned.hash, buf, sizeof buf); CHECK(n >= 96 && (n - 96) % 65 == 0);
    CHECK(commit_root(h, root) == 0); CHECK(commit_verify(buf, n, root) == 1);
    buf[40] ^= 1; CHECK(commit_verify(buf, n, root) == 0);
    CHECK(commit_proof(h, pinned.hash, buf, 10) == -2);
    u8 half[32]; be32(5, half); CHECK(commit_set_remaining(h, pinned.hash, half) == 1);
    u8 root2[32]; commit_root(h, root2); CHECK(std::memcmp(root, root2, 32) != 0);
    CHECK(commit_remove(h, pinned.hash) == 1); CHECK(commit_remove(h, pinned.hash) == 0); CHECK(commit_size(h) == 49);
    CHECK(commit_proof(h, pinned.hash, buf, sizeof buf) == -1);
    CHECK(commit_set_remaining(h, pinned.hash, half) == 0);
    CHECK(commit_insert(nullptr, pinned.hash, pinned.user, 1, pinned.price, pinned.remaining, 7) == -1);
    CHECK(commit_root(nullptr, root) == -1); CHECK(commit_size(nullptr) == 0);
    commit_free(h); commit_free(nullptr);
  }
  printf("%s %d checks, %d failures\n", failures ? "FAILED:" : "ok:", checks, failures);
  return failures ? 1 : 0;
}
