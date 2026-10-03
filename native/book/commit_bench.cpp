// native/book/commit_bench.cpp — the commit tree under load: N inserts, N/2 partial fills, N/2 removes, each
// phase timed, plus the cost of one keccak256 of a node encoding (the floor every operation is a multiple of).
//   ./commit_bench [n=1000000]
#include "commit.hpp"
#include "keccak.hpp"
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <vector>

using namespace rollcommit;
struct Rng { u64 s; explicit Rng(u64 seed) : s(seed) {} u64 next() { s ^= s >> 12; s ^= s << 25; s ^= s >> 27; return s * 2685821657736338717ull; } void fill(u8* p, size_t n) { for (size_t i = 0; i < n; ++i) p[i] = (u8)(next() >> 56); } };
static void be32(u64 v, u8 out[32]) { std::memset(out, 0, 32); for (int i = 0; i < 8; ++i) { out[31 - i] = (u8)v; v >>= 8; } }
using clk = std::chrono::steady_clock;
static double us_since(clk::time_point t0) { return std::chrono::duration<double, std::micro>(clk::now() - t0).count(); }

int main(int argc, char** argv) {
  const size_t n = argc > 1 ? (size_t)strtoull(argv[1], nullptr, 10) : 1000000;
  Rng r(0x5eed);
  // the keccak floor: a 97-byte node encoding, one permutation
  { u8 b[97], h[32]; r.fill(b, 97); auto t0 = clk::now(); for (int i = 0; i < 200000; ++i) { keccak::hash256(b, 97, h); b[0] = h[0]; } double us = us_since(t0) / 200000; printf("keccak256(97 bytes)      %8.3f µs   (%.1f ns)\n", us, us * 1000); }

  CommitTree t(n);
  std::vector<u8> hashes(n * 32);
  std::vector<u8> ladder(32);
  // phase 1: n inserts (prices over 10,000 ticks, 1e14-scaled; sizes 1..100e18; seq = i)
  auto t0 = clk::now();
  for (size_t i = 0; i < n; ++i) {
    Entry e; r.fill(e.hash, 32); r.fill(e.user, 20); e.buy = (r.next() & 1) != 0;
    be32((1 + r.next() % 10000) * 100000000000000ull, e.price);
    be32((1 + r.next() % 100) * 1000000000000000000ull, e.remaining);
    e.seq = i + 1;
    std::memcpy(&hashes[i * 32], e.hash, 32);
    t.insert(e);
  }
  double ins = us_since(t0); u64 k1 = t.keccaks;
  printf("insert       %9zu   %8.3f µs/op   %5.1f keccaks/op   size %zu\n", n, ins / n, (double)k1 / n, t.size());
  // phase 2: n/2 partial fills on random live orders
  const size_t half = n / 2; std::vector<size_t> live(n); for (size_t i = 0; i < n; ++i) live[i] = i;
  t0 = clk::now(); u64 k0 = t.keccaks;
  for (size_t i = 0; i < half; ++i) { size_t k = r.next() % live.size(); u8 rem[32]; be32(1 + r.next() % 100000000000000000ull, rem); t.set_remaining(&hashes[live[k] * 32], rem); }
  double upd = us_since(t0);
  printf("set_remaining %8zu   %8.3f µs/op   %5.1f keccaks/op\n", half, upd / half, (double)(t.keccaks - k0) / half);
  // phase 3: n/2 removes of random live orders
  t0 = clk::now(); k0 = t.keccaks;
  for (size_t i = 0; i < half; ++i) { size_t k = r.next() % live.size(); t.remove(&hashes[live[k] * 32]); live[k] = live.back(); live.pop_back(); }
  double rem = us_since(t0);
  printf("remove       %9zu   %8.3f µs/op   %5.1f keccaks/op   size %zu\n", half, rem / half, (double)(t.keccaks - k0) / half, t.size());
  // the root is a read
  t0 = clk::now(); u8 root[32]; for (int i = 0; i < 1000000; ++i) { t.root(root); root[0] ^= 1; } double rd = us_since(t0) / 1000000; t.root(root);
  printf("root()                   %8.4f µs\n", rd);
  // a proof, and its verification
  Proof p; t0 = clk::now(); for (size_t i = 0; i < 100000; ++i) t.proof(&hashes[live[r.next() % live.size()] * 32], p); double pr = us_since(t0) / 100000;
  t0 = clk::now(); bool okv = true; for (int i = 0; i < 100000; ++i) okv = okv && verify(p, root); double vf = us_since(t0) / 100000;
  printf("proof()                  %8.3f µs   verify() %8.3f µs   depth %zu   %s\n", pr, vf, p.path.size(), okv ? "ok" : "BAD");
  printf("root 0x"); for (int i = 0; i < 32; ++i) printf("%02x", root[i]); printf("   size %zu   keccaks %llu\n", t.size(), (unsigned long long)t.keccaks);
  return 0;
}
