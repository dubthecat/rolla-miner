// native/book/test.cpp — the native book's unit tests. The first five mirror engine/l3/matcher.test.mjs one
// for one (the JS book is the specification); the rest cover what only the native book has: the tick domain,
// the id table, the slab free list, the bitmap edges, checked arithmetic and the two hashes.
//   ./book_test        — prints one line per test, exits non-zero on the first failure
#include "book.hpp"
#include <cstdio>
#include <cstring>
#include <vector>

using namespace rollbook;
static int failures = 0, checks = 0;
static const char* current = "";
#define CHECK(cond) do { ++checks; if (!(cond)) { ++failures; printf("  FAIL %s:%d  %s\n", current, __LINE__, #cond); } } while (0)
#define TEST(name) current = name; printf("· %s\n", name);

static const u128 E18 = (u128)1000000000000000000ull;
static i64 tk(double price) { return (i64)(price * 10000.0 + 0.5); }  // 0.55 → tick 5500 at a tick size of 1e14
static u128 sz(unsigned n) { return (u128)n * E18; }

int main() {
  {
    TEST("a crossing taker fills at the MAKER price, FIFO within a level, best price first");
    Book b;
    b.add(1, 1 /*ann*/, false, tk(0.60), sz(10), F_NONE);
    b.add(2, 2 /*bob*/, false, tk(0.55), sz(5), F_NONE);
    b.add(3, 3 /*cat*/, false, tk(0.55), sz(5), F_NONE);
    auto r = b.add(4, 4 /*dan*/, true, tk(0.60), sz(12), F_NONE);
    CHECK(r.nfills == 3); CHECK(!r.rested); CHECK(r.remaining == 0);
    const Fill* f = b.fills();
    CHECK(f[0].maker_user == 2 && f[0].tick == tk(0.55) && f[0].size == sz(5));
    CHECK(f[1].maker_user == 3 && f[1].tick == tk(0.55) && f[1].size == sz(5));
    CHECK(f[2].maker_user == 1 && f[2].tick == tk(0.60) && f[2].size == sz(2));
    CHECK(f[0].seq < f[1].seq && f[1].seq < f[2].seq);
    CHECK(b.best_ask() == tk(0.60)); CHECK(b.best_bid() == NO_TICK);
    std::vector<LevelView> bids, asks; b.depth(5, bids, asks);
    CHECK(asks.size() == 1 && asks[0].total == sz(8) && asks[0].count == 1);
  }
  {
    TEST("what does not cross rests; post-only that would cross is refused; ioc never rests");
    Book b;
    CHECK(b.add(1, 1, true, tk(0.40), sz(10), F_NONE).rested);
    auto po = b.add(2, 2, false, tk(0.40), sz(1), F_POST_ONLY);
    CHECK(po.reason == R_WOULD_CROSS); CHECK(!po.rested); CHECK(po.remaining == sz(1)); CHECK(po.nfills == 0);
    CHECK(b.add(3, 2, false, tk(0.45), sz(1), F_POST_ONLY).rested);
    auto ioc = b.add(4, 3, false, tk(0.30), sz(25), F_IOC);
    CHECK(ioc.nfills == 1); CHECK(ioc.remaining == sz(15)); CHECK(!ioc.rested);
    CHECK(b.size() == 1);                          // only order 3 is left
  }
  {
    TEST("self-trades are allowed: an order fills against the same user's resting orders in strict price-time");
    Book b;
    b.add(1, 1 /*ann*/, false, tk(0.50), sz(5), F_NONE);
    b.add(2, 2 /*bob*/, false, tk(0.50), sz(5), F_NONE);
    b.add(3, 1 /*ann*/, false, tk(0.52), sz(5), F_NONE);
    auto r = b.add(4, 1 /*ann*/, true, tk(0.52), sz(7), F_NONE);
    CHECK(r.nfills == 2);
    CHECK(b.fills()[0].maker_user == 1 && b.fills()[0].size == sz(5));   // ann's own order first: it is at the front
    CHECK(b.fills()[1].maker_user == 2 && b.fills()[1].size == sz(2));
    CHECK(r.remaining == 0); CHECK(!r.rested);
    std::vector<OrderView> mine; b.orders(1, true, mine);
    CHECK(mine.size() == 1);                       // the 0.52 ask is untouched; the 0.50 one is gone
    CHECK(b.best_ask() == tk(0.50));               // bob's remaining 3 at 0.50
  }
  {
    TEST("cancel removes exactly that order and empties its level");
    Book b;
    b.add(1, 1, true, tk(0.30), sz(4), F_NONE); b.add(2, 2, true, tk(0.30), sz(6), F_NONE);
    auto c = b.cancel(1); CHECK(c.found && c.id == 1 && c.remaining == sz(4));
    CHECK(!b.cancel(1).found);
    std::vector<LevelView> bids, asks; b.depth(5, bids, asks);
    CHECK(bids.size() == 1 && bids[0].total == sz(6) && bids[0].count == 1);
    CHECK(b.cancel(2).found);
    b.depth(5, bids, asks); CHECK(bids.empty()); CHECK(b.best_bid() == NO_TICK); CHECK(b.size() == 0);
  }
  {
    TEST("duplicate ids are refused and a preview never changes the book");
    Book b;
    b.add(1, 1, false, tk(0.70), sz(3), F_NONE);
    auto d = b.add(1, 1, false, tk(0.70), sz(3), F_NONE);
    CHECK(d.reason == R_DUPLICATE); CHECK(d.remaining == sz(3)); CHECK(d.seq == 0);
    u64 seq = b.seq(), h = b.state_hash();
    auto p = b.preview(true, tk(0.70), sz(5));
    CHECK(p.filled == sz(3)); CHECK(p.fills == 1);
    // cost = 3e18 * 0.7e18 = 2.1e36, which needs more than 64 bits: limbs 0 and 1 together
    u128 cost = ((u128)p.cost[1] << 64) | p.cost[0];
    CHECK(p.cost[2] == 0 && p.cost[3] == 0);
    CHECK(cost == sz(3) * (u128)700000000000000000ull);
    CHECK(b.size() == 1); CHECK(b.seq() == seq); CHECK(b.state_hash() == h);   // a quote is not an event
    // a quote includes the asker's own resting orders, because a self-trade is a trade (the C ABI still takes
    // a user, and ignores it, so that book_preview matches createBook().preview's signature)
    BookPreviewOut po{};
    book_preview((void*)&b, 1, tk(0.70), (uint64_t)sz(5), 0, 1 /*the owner*/, 1, &po);
    CHECK(((u128)po.filled_hi << 64 | po.filled_lo) == sz(3));
  }
  {
    TEST("a partial fill leaves the maker at the front of its level with the remainder");
    Book b;
    b.add(1, 1, false, tk(0.50), sz(10), F_NONE);
    b.add(2, 2, false, tk(0.50), sz(10), F_NONE);
    auto r = b.add(3, 3, true, tk(0.50), sz(4), F_NONE);
    CHECK(r.nfills == 1 && r.remaining == 0 && !r.rested);
    OrderView o{}; CHECK(b.get(1, o) && o.remaining == sz(6) && o.size == sz(10));
    auto r2 = b.add(4, 4, true, tk(0.50), sz(7), F_NONE);     // 6 from order 1, then 1 from order 2
    CHECK(r2.nfills == 2); CHECK(b.fills()[0].maker_id == 1 && b.fills()[0].size == sz(6));
    CHECK(b.fills()[1].maker_id == 2 && b.fills()[1].size == sz(1));
    CHECK(!b.get(1, o)); CHECK(b.get(2, o) && o.remaining == sz(9));
  }
  {
    TEST("bad orders are refused without spending a sequence number");
    Book b;
    CHECK(b.add(1, 1, true, 0, sz(1), F_NONE).reason == R_BAD);          // price 0
    CHECK(b.add(2, 1, true, -5, sz(1), F_NONE).reason == R_BAD);         // negative tick
    CHECK(b.add(3, 1, true, tk(0.5), 0, F_NONE).reason == R_BAD);        // zero size
    CHECK(b.add(0, 1, true, tk(0.5), sz(1), F_NONE).reason == R_BAD);    // id 0 is the table's empty sentinel
    CHECK(b.add(~0ull, 1, true, tk(0.5), sz(1), F_NONE).reason == R_BAD);
    CHECK(b.seq() == 0); CHECK(b.size() == 0);
    // the tick domain is bounded: an outcome book refuses a price above 1.0 rather than growing
    Book o(DEFAULT_TICK_SIZE, DEFAULT_TICK_CAP, DEFAULT_TICK_CAP - 1);
    CHECK(o.add(1, 1, true, 10000, sz(1), F_NONE).rested);               // exactly 1.0
    CHECK(o.add(2, 1, true, 10001, sz(1), F_NONE).reason == R_DOMAIN);
    CHECK(o.seq() == 1);
    // ... while a book for another instrument grows its flat array on demand
    Book g(DEFAULT_TICK_SIZE, 64, MAX_TICK_CAP - 1);
    CHECK(g.add(1, 1, true, 1000000, sz(1), F_NONE).rested);
    CHECK(g.best_bid() == 1000000); CHECK(g.tick_cap() > 1000000);
    CHECK(g.add(2, 1, true, MAX_TICK_CAP, sz(1), F_NONE).reason == R_DOMAIN);
  }
  {
    TEST("checked arithmetic: a level total that would wrap is refused, sizes above 2^64 are fine");
    Book b;
    const u128 huge = ((u128)1 << 127);
    CHECK(b.add(1, 1, true, tk(0.5), huge, F_NONE).rested);              // the level total is 2^127
    auto r = b.add(2, 2, true, tk(0.5), huge, F_NONE);                   // 2^127 + 2^127 would wrap to zero
    CHECK(r.reason == R_OVERFLOW); CHECK(!r.rested); CHECK(r.remaining == huge);
    CHECK(b.add(3, 3, true, tk(0.5), huge - 1, F_NONE).rested);          // one unit less fits
    std::vector<LevelView> bids, asks; b.depth(1, bids, asks);
    CHECK(bids[0].total == huge + (huge - 1)); CHECK(bids[0].count == 2);
    // a 128-bit size round-trips through a fill
    Book c;
    c.add(1, 1, false, tk(0.5), huge, F_NONE);
    auto f = c.add(2, 2, true, tk(0.5), huge, F_NONE);
    CHECK(f.nfills == 1 && c.fills()[0].size == huge && f.remaining == 0);
  }
  {
    TEST("best bid / best ask over the whole tick domain, including the first and last tick");
    Book b;
    CHECK(b.best_bid() == NO_TICK && b.best_ask() == NO_TICK);
    b.add(1, 1, true, 1, sz(1), F_NONE); CHECK(b.best_bid() == 1);
    b.add(2, 1, true, 10000, sz(1), F_NONE); CHECK(b.best_bid() == 10000);
    b.add(3, 1, true, 5000, sz(1), F_NONE); CHECK(b.best_bid() == 10000);
    b.cancel(2); CHECK(b.best_bid() == 5000);
    b.cancel(3); CHECK(b.best_bid() == 1);
    b.cancel(1); CHECK(b.best_bid() == NO_TICK);
    for (i64 t = 1; t <= 10000; ++t) b.add((u64)t + 100, 1, false, t, sz(1), F_NONE);
    CHECK(b.best_ask() == 1);
    std::vector<LevelView> bids, asks; b.depth(3, bids, asks);
    CHECK(asks.size() == 3 && asks[0].tick == 1 && asks[1].tick == 2 && asks[2].tick == 3);
    for (i64 t = 1; t <= 9999; ++t) b.cancel((u64)t + 100);
    CHECK(b.best_ask() == 10000); CHECK(b.size() == 1);
  }
  {
    TEST("a taker sweeps its own levels in price order like any other");
    Book b;
    b.add(1, 1, false, tk(0.50), sz(5), F_NONE);        // own
    b.add(2, 1, false, tk(0.51), sz(5), F_NONE);        // own
    b.add(3, 2, false, tk(0.52), sz(5), F_NONE);        // someone else, two levels further out
    auto r = b.add(4, 1, true, tk(0.52), sz(12), F_NONE);
    CHECK(r.nfills == 3);
    CHECK(b.fills()[0].tick == tk(0.50) && b.fills()[1].tick == tk(0.51) && b.fills()[2].tick == tk(0.52));
    CHECK(b.fills()[2].size == sz(2)); CHECK(r.remaining == 0); CHECK(b.size() == 1);
    CHECK(b.best_ask() == tk(0.52));
  }
  {
    TEST("the slab reuses freed slots: 200k orders through a book that never holds more than one");
    Book b; bool ok = true;
    for (u64 i = 1; i <= 200000; ++i) { ok = ok && b.add(i, 1, true, tk(0.42), sz(1), F_NONE).rested && b.cancel(i).found; }
    CHECK(ok); CHECK(b.size() == 0); CHECK(b.best_bid() == NO_TICK);
    std::vector<LevelView> bids, asks; b.depth(1, bids, asks); CHECK(bids.empty());
  }
  {
    TEST("determinism: two books fed the same requests agree on fills, sequence numbers and both hashes");
    Book a, b;
    uint32_t s1 = 99, s2 = 99;
    auto step = [](Book& bk, uint32_t& s, int i, std::vector<u64>& live) {
      auto rnd = [&s]() { s = (uint32_t)(s * 1664525u + 1013904223u); return (double)s / 4294967296.0; };
      if (!live.empty() && rnd() < 0.2) { size_t k = (size_t)(rnd() * (double)live.size()); u64 id = live[k]; live.erase(live.begin() + (long)k); bk.cancel(id); return; }
      bool buy = rnd() < 0.5; i64 t = 4000 + (i64)(rnd() * 2000.0); u128 q = (u128)(1 + (int)(rnd() * 50.0)) * E18;
      auto r = bk.add((u64)i + 1, 1 + (u64)(rnd() * 6.0), buy, t, q, (rnd() < 0.1) ? F_IOC : F_NONE);
      if (r.rested) live.push_back((u64)i + 1);
    };
    std::vector<u64> l1, l2;
    for (int i = 0; i < 50000; ++i) step(a, s1, i, l1);
    for (int i = 0; i < 50000; ++i) step(b, s2, i, l2);
    CHECK(a.seq() == b.seq()); CHECK(a.size() == b.size());
    CHECK(a.state_hash() == b.state_hash()); CHECK(a.book_hash() == b.book_hash());
    CHECK(a.size() > 1000);
    // and a book that reached the same state by a different route has the same book hash but not the same transcript
    Book c, d;
    c.add(1, 1, true, 4000, sz(5), F_NONE); c.add(2, 2, true, 4000, sz(5), F_NONE);
    d.add(2, 2, true, 4000, sz(5), F_NONE); d.add(9, 9, true, 4000, sz(5), F_NONE); d.cancel(9); d.add(1, 1, true, 4000, sz(5), F_NONE);
    CHECK(c.book_hash() != d.book_hash());            // FIFO order differs, so the books really are different
    CHECK(c.state_hash() != d.state_hash());
  }
  printf("%s %d checks, %d failures\n", failures ? "FAILED:" : "ok:", checks, failures);
  return failures ? 1 : 0;
}
