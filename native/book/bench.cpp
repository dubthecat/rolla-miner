// native/book/bench.cpp — throughput, latency and memory for the native book, single thread.
//
//   ./bench mixed <ops> [seed] [span] [step]
//                           the mixed workload: 30% of new orders cross, 10% of ops are cancels, 10% IOC,
//                           prices on a 1e14 tick grid around a random-walking mid, sizes 1..100 × 1e18.
//                           One latency sample every 1,000 ops; p50/p99/p99.9/max; peak RSS; per-segment rate.
//   ./bench patho <depth>   one price level with <depth> resting orders: sweep it with a single crossing
//                           order, then rebuild it and cancel all of them in random order.
//   ./bench timer           the cost of the clock itself, which bounds how small a p50 can mean anything.
//
// bench.mjs runs the identical mixed workload through engine/l3/matcher.js — same LCG, same draw order, same
// swap-removal of the live list — so the two rates are comparable. The live list is swap-removed here (unlike
// trace.cpp, which must reproduce matcher.test.mjs's O(n) splice exactly): an O(n) harness would otherwise
// dominate the measurement at 10M ops.
#include "book.hpp"
#include <cstdio>
#include <cstring>
#include <cstdlib>
#include <cmath>
#include <chrono>
#include <algorithm>
#include <vector>

using namespace rollbook;
using clk = std::chrono::steady_clock;
static const u128 E18 = (u128)1000000000000000000ull;
static double secs(clk::time_point a, clk::time_point b) { return std::chrono::duration<double>(b - a).count(); }

static long peak_rss_kb() {
  FILE* f = fopen("/proc/self/status", "r"); if (!f) return -1;
  char line[256]; long kb = -1;
  while (fgets(line, sizeof line, f)) if (!strncmp(line, "VmHWM:", 6)) { sscanf(line + 6, "%ld", &kb); break; }
  fclose(f); return kb;
}
static double pct(std::vector<double>& v, double p) {
  if (v.empty()) return 0;
  size_t i = (size_t)(p * (double)(v.size() - 1) + 0.5);
  return v[i];
}
static void report_lat(const char* what, std::vector<double> ns) {
  std::sort(ns.begin(), ns.end());
  printf("  %s latency (n=%zu): p50 %.2f µs · p99 %.2f µs · p99.9 %.2f µs · max %.2f µs\n", what, ns.size(),
         pct(ns, 0.50) / 1000.0, pct(ns, 0.99) / 1000.0, pct(ns, 0.999) / 1000.0, ns.empty() ? 0.0 : ns.back() / 1000.0);
}

// the shared generator: see bench.mjs for the line-by-line twin
struct Gen {
  uint32_t s;
  explicit Gen(uint32_t seed) : s(seed) {}
  double rnd() { s = (uint32_t)(s * 1664525u + 1013904223u); return (double)s / 4294967296.0; }
};

static void mixed(long long ops, int seed, i64 span, i64 step) {
  Book book(DEFAULT_TICK_SIZE, DEFAULT_TICK_CAP, DEFAULT_TICK_CAP - 1, 1u << 20);
  Gen g((uint32_t)seed);
  std::vector<u64> live; live.reserve((size_t)ops);
  std::vector<double> lat; lat.reserve((size_t)(ops / 1000 + 2));
  i64 mid = 5000;
  long long nfills = 0, ncancels = 0, nhit = 0;
  const long long seg = ops / 10 > 0 ? ops / 10 : 1;
  std::vector<double> seg_rate;
  auto t0 = clk::now(); auto tseg = t0;

  for (long long i = 0; i < ops; ++i) {
    const bool sample = (i % 1000 == 0);
    clk::time_point s0;
    bool is_cancel = !live.empty() && g.rnd() < 0.1;
    if (is_cancel) {
      size_t k = (size_t)(g.rnd() * (double)live.size());
      u64 id = live[k]; live[k] = live.back(); live.pop_back();
      if (sample) s0 = clk::now();
      bool hit = book.cancel(id).found;
      if (sample) lat.push_back((double)std::chrono::duration_cast<std::chrono::nanoseconds>(clk::now() - s0).count());
      ++ncancels; nhit += hit ? 1 : 0;
    } else {
      bool buy = g.rnd() < 0.5;
      bool cross = g.rnd() < 0.3;
      i64 off = (i64)(g.rnd() * (double)span) * step;
      i64 tick = buy ? (cross ? mid + off : mid - 1 - off) : (cross ? mid - off : mid + 1 + off);
      tick = tick < 1 ? 1 : (tick > 9999 ? 9999 : tick);
      u64 user = 1 + (u64)(g.rnd() * 4096.0);
      u128 size = (u128)(1 + (int)(g.rnd() * 100.0)) * E18;
      u32 flags = g.rnd() < 0.1 ? F_IOC : F_NONE;
      mid += (g.rnd() < 0.5 ? -1 : 1);
      mid = mid < 2500 ? 2500 : (mid > 7500 ? 7500 : mid);
      if (sample) s0 = clk::now();
      auto r = book.add((u64)i + 1, user, buy, tick, size, flags);
      if (sample) lat.push_back((double)std::chrono::duration_cast<std::chrono::nanoseconds>(clk::now() - s0).count());
      nfills += r.nfills;
      if (r.rested) live.push_back((u64)i + 1);
    }
    if ((i + 1) % seg == 0) { auto now = clk::now(); seg_rate.push_back((double)seg / secs(tseg, now)); tseg = now; }
  }
  double dt = secs(t0, clk::now());
  printf("mixed %lld ops in %.3f s → %.0f ops/s (span %lld × step %lld)\n", ops, dt, (double)ops / dt, (long long)span, (long long)step);
  printf("  %lld fills · %lld cancels (%lld hit) · %zu resting · %lld levels wide · seq %llu · transcript %016llx\n",
         nfills, ncancels, nhit, book.size(), (long long)book.tick_cap(), (unsigned long long)book.seq(), (unsigned long long)book.state_hash());
  report_lat("per-op", lat);
  printf("  peak RSS %.1f MB\n", (double)peak_rss_kb() / 1024.0);
  printf("  ops/s per 10%% segment:");
  for (double r : seg_rate) printf(" %.0f", r);
  printf("\n");
}

static void patho(long long depth) {
  // (a) sweep: `depth` resting orders at one price, taken out by a single crossing order
  {
    Book book(DEFAULT_TICK_SIZE, DEFAULT_TICK_CAP, DEFAULT_TICK_CAP - 1, (size_t)depth + 16);
    auto t0 = clk::now();
    for (long long i = 0; i < depth; ++i) book.add((u64)i + 1, (u64)(i % 4096) + 1, false, 5000, E18, F_NONE);
    double tb = secs(t0, clk::now());
    auto t1 = clk::now();
    auto r = book.add((u64)depth + 1, 999999, true, 5000, (u128)depth * E18, F_NONE);
    double ts = secs(t1, clk::now());
    printf("patho sweep depth %lld: build %.3f s (%.0f adds/s) · sweep %lld fills in %.3f s → %.0f fills/s (%.0f ns/fill)\n",
           depth, tb, (double)depth / tb, (long long)r.nfills, ts, (double)r.nfills / ts, ts * 1e9 / (double)r.nfills);
    printf("  book empty after the sweep: %s · remaining %s\n", book.size() == 0 ? "yes" : "NO", r.remaining == 0 ? "0" : "nonzero");
  }
  // (b) cancel-all: the same level, cancelled in random order
  {
    Book book(DEFAULT_TICK_SIZE, DEFAULT_TICK_CAP, DEFAULT_TICK_CAP - 1, (size_t)depth + 16);
    for (long long i = 0; i < depth; ++i) book.add((u64)i + 1, (u64)(i % 4096) + 1, false, 5000, E18, F_NONE);
    std::vector<u64> ids((size_t)depth);
    for (long long i = 0; i < depth; ++i) ids[(size_t)i] = (u64)i + 1;
    Gen g(12345);
    for (long long i = depth - 1; i > 0; --i) { size_t j = (size_t)(g.rnd() * (double)(i + 1)); std::swap(ids[(size_t)i], ids[j]); }
    std::vector<double> lat; lat.reserve((size_t)depth);
    auto t0 = clk::now();
    for (long long i = 0; i < depth; ++i) {
      auto s0 = clk::now();
      book.cancel(ids[(size_t)i]);
      lat.push_back((double)std::chrono::duration_cast<std::chrono::nanoseconds>(clk::now() - s0).count());
    }
    double dt = secs(t0, clk::now());
    printf("patho cancel-all depth %lld: %.3f s → %.0f cancels/s · book %zu\n", depth, dt, (double)depth / dt, book.size());
    report_lat("cancel", lat);
    printf("  peak RSS %.1f MB\n", (double)peak_rss_kb() / 1024.0);
  }
}

static void timer_cost() {
  std::vector<double> v; v.reserve(100000);
  for (int i = 0; i < 100000; ++i) { auto a = clk::now(); auto b = clk::now(); v.push_back((double)std::chrono::duration_cast<std::chrono::nanoseconds>(b - a).count()); }
  std::sort(v.begin(), v.end());
  printf("steady_clock::now() pair cost: p50 %.0f ns · p99 %.0f ns\n", pct(v, 0.5), pct(v, 0.99));
}

int main(int argc, char** argv) {
  const char* what = argc > 1 ? argv[1] : "mixed";
  long long n = argc > 2 ? atoll(argv[2]) : 1000000;
  int seed = argc > 3 ? atoi(argv[3]) : 7;
  i64 span = argc > 4 ? atoll(argv[4]) : 2000;      // how many distinct offsets a new order may sit at
  i64 step = argc > 5 ? atoll(argv[5]) : 1;         // the offset granularity in ticks (span 20 × step 100 is
  if (!strcmp(what, "mixed")) mixed(n, seed, span, step);   // the 41-level cents grid of the earlier review)
  else if (!strcmp(what, "patho")) patho(n);
  else if (!strcmp(what, "timer")) timer_cost();
  else { fprintf(stderr, "usage: bench [mixed <ops> | patho <depth> | timer]\n"); return 2; }
  return 0;
}
