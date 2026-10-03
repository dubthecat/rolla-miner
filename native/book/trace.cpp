// native/book/trace.cpp — replay the deterministic workload of engine/l3/matcher.test.mjs's run(seed, count)
// against the native book and print the fill/cancel lines in matcher.test.mjs's own format, so diff.mjs can
// compare them line for line with the JavaScript reference book.
//
// The generator is a line-by-line port, including the order in which the LCG is drawn (which is observable:
// the cancel branch only draws when the live list is non-empty, and the object literal draws user, then size,
// then ioc, after buy/cross/off). Prices are cents 30..70; the native book takes them as ticks = cents * 100
// at the default tick size of 1e14, which is the same 1e18-scaled price the JS book holds as a bigint.
//
//   ./trace <seed> <count> [--summary] [--quiet]
#include "book.hpp"
#include <cstdio>
#include <cmath>
#include <cstring>
#include <string>
#include <vector>

using namespace rollbook;
static const u128 E18 = (u128)1000000000000000000ull;

int main(int argc, char** argv) {
  u64 seed = argc > 1 ? strtoull(argv[1], nullptr, 10) : 7;
  long long count = argc > 2 ? atoll(argv[2]) : 20000;
  bool summary = false, quiet = false;
  for (int i = 3; i < argc; ++i) { if (!strcmp(argv[i], "--summary")) summary = true; if (!strcmp(argv[i], "--quiet")) quiet = true; }

  Book book(DEFAULT_TICK_SIZE, DEFAULT_TICK_CAP, DEFAULT_TICK_CAP - 1, 1 << 16);
  uint32_t s = (uint32_t)seed;
  auto rnd = [&s]() -> double { s = (uint32_t)(s * 1664525u + 1013904223u); return (double)s / 4294967296.0; };

  std::vector<u64> live;                  // the indices of orders that rested, in the JS order (spliced out at random)
  std::string out; out.reserve(1 << 22);
  char buf[64];
  long long lines = 0;

  for (long long i = 0; i < count; ++i) {
    if (!live.empty() && rnd() < 0.1) {
      size_t idx = (size_t)std::floor(rnd() * (double)live.size());
      u64 h = live[idx]; live.erase(live.begin() + (long)idx);
      auto c = book.cancel(h + 1);        // id = index + 1: id 0 is the hash table's empty sentinel
      ++lines;
      if (!quiet) { out += c.found ? 'c' : 'x'; out += 'o'; out += std::to_string(h); out += '\n'; }
      continue;
    }
    bool buy = rnd() < 0.5;
    bool cross = rnd() < 0.3;
    const int mid = 50;
    int off = (int)std::floor(rnd() * 20.0);
    int price = buy ? (cross ? mid + off : mid - 1 - off) : (cross ? mid - off : mid + 1 + off);
    u64 user = 1 + (u64)std::floor(rnd() * 8.0);
    u128 size = (u128)(1 + (int)std::floor(rnd() * 100.0)) * E18;
    bool ioc = rnd() < 0.1;

    auto r = book.add((u64)i + 1, user, buy, (i64)price * 100, size, ioc ? F_IOC : F_NONE);
    if (r.rested) live.push_back((u64)i);
    const Fill* f = book.fills();
    for (size_t k = 0; k < book.fill_count(); ++k) {
      ++lines;
      if (quiet) continue;
      out += 'o'; out += std::to_string(f[k].maker_id - 1);
      out += '>'; out += 'o'; out += std::to_string(f[k].taker_id - 1);
      out += '@'; u128_dec((u128)f[k].tick * (u128)book.tick_size(), buf); out += buf;
      out += ':'; u128_dec(f[k].size, buf); out += buf;
      out += '\n';
    }
    if (out.size() > (1u << 20)) { fwrite(out.data(), 1, out.size(), stdout); out.clear(); }
  }
  if (!out.empty()) fwrite(out.data(), 1, out.size(), stdout);
  if (summary)
    printf("# ops=%lld lines=%lld resting=%zu seq=%llu thash=%016llx bhash=%016llx\n",
           count, lines, book.size(), (unsigned long long)book.seq(),
           (unsigned long long)book.state_hash(), (unsigned long long)book.book_hash());
  return 0;
}
