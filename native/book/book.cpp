// native/book/book.cpp — the implementation. See book.hpp for the contract and L3-NATIVE-BOOK.md for the design.
#include "book.hpp"
#include <cstring>
#include <cstdlib>
#include <algorithm>

namespace rollbook {

static inline int ctz64(u64 w) noexcept { return __builtin_ctzll(w); }
static inline int clz64(u64 w) noexcept { return __builtin_clzll(w); }

// ------------------------------------------------------------------ TickBitmap
void TickBitmap::reset(size_t ticks) {
  cap_ = ticks;
  size_t b = ticks;
  for (int k = 0; k < 5; ++k) {
    bits_[k] = b;
    size_t words = (b + 63) / 64; if (words == 0) words = 1;
    std::vector<u64> next(words, 0ull);
    // growing: keep the words we already had (a tick's index never moves, so the words are still valid)
    for (size_t i = 0; i < std::min(words, lv_[k].size()); ++i) next[i] = lv_[k][i];
    lv_[k].swap(next);
    b = words;
  }
}
void TickBitmap::set(size_t i) noexcept {
  for (int k = 0; k < 5; ++k) { lv_[k][i >> 6] |= (1ull << (i & 63)); i >>= 6; }
}
void TickBitmap::clear(size_t i) noexcept {
  for (int k = 0; k < 5; ++k) {
    u64& w = lv_[k][i >> 6]; w &= ~(1ull << (i & 63));
    if (w) return;                      // the word above still has other bits set below it
    i >>= 6;
  }
}
i64 TickBitmap::next_set(size_t from) const noexcept {
  size_t i = from;
  for (int k = 0; k < 5; ++k) {
    if (i >= bits_[k]) return -1;
    size_t wi = i >> 6;
    u64 w = lv_[k][wi] & (~0ull << (i & 63));
    if (w) {
      size_t idx = wi * 64 + (size_t)ctz64(w);
      for (int d = k; d > 0; --d) idx = idx * 64 + (size_t)ctz64(lv_[d - 1][idx]);
      return (i64)idx;
    }
    i = wi + 1;
  }
  return -1;
}
i64 TickBitmap::prev_set(size_t from) const noexcept {
  size_t i = from;
  for (int k = 0; k < 5; ++k) {
    if (i >= bits_[k]) { if (bits_[k] == 0) return -1; i = bits_[k] - 1; }
    size_t wi = i >> 6, bi = i & 63;
    u64 w = lv_[k][wi] & (bi == 63 ? ~0ull : ((1ull << (bi + 1)) - 1));
    if (w) {
      size_t idx = wi * 64 + (size_t)(63 - clz64(w));
      for (int d = k; d > 0; --d) { u64 ww = lv_[d - 1][idx]; idx = idx * 64 + (size_t)(63 - clz64(ww)); }
      return (i64)idx;
    }
    if (wi == 0) return -1;
    i = wi - 1;
  }
  return -1;
}

// ------------------------------------------------------------------ Book
Book::Book(i64 tick_size, i64 tick_cap, i64 tick_limit, size_t slab_hint)
    : tick_size_(tick_size > 0 ? tick_size : DEFAULT_TICK_SIZE),
      tick_limit_(tick_limit < 0 ? 0 : (tick_limit >= MAX_TICK_CAP ? MAX_TICK_CAP - 1 : tick_limit)) {
  i64 cap = tick_cap < 1 ? 1 : tick_cap;
  if (cap > tick_limit_ + 1) cap = tick_limit_ + 1;
  bid_levels_.assign((size_t)cap, Level{});
  ask_levels_.assign((size_t)cap, Level{});
  bid_map_.reset((size_t)cap);
  ask_map_.reset((size_t)cap);
  slab_.reserve(slab_hint ? slab_hint : 1024);
  fills_.reserve(256);
  table_reset(4096);
}

bool Book::ensure_tick(i64 tick) noexcept {
  if (tick < 0 || tick > tick_limit_) return false;
  if ((size_t)tick < bid_levels_.size()) return true;
  size_t want = (size_t)tick + 1, cap = bid_levels_.size();
  while (cap < want) cap *= 2;
  if (cap > (size_t)tick_limit_ + 1) cap = (size_t)tick_limit_ + 1;
  bid_levels_.resize(cap);
  ask_levels_.resize(cap);
  bid_map_.reset(cap);
  ask_map_.reset(cap);
  return true;
}

// --- slab
u32 Book::alloc_slot() noexcept {
  if (free_head_ != NIL) { u32 s = free_head_; free_head_ = slab_[s].next; return s; }
  if (slab_.size() >= (size_t)NIL - 1) return NIL;
  slab_.push_back(Order{});
  return (u32)(slab_.size() - 1);
}
void Book::free_slot(u32 s) noexcept { slab_[s].live = false; slab_[s].next = free_head_; free_head_ = s; }

// --- id → slot: open addressing, linear probing, tombstones
void Book::table_reset(size_t slots) {
  size_t n = 64; while (n < slots) n <<= 1;
  tkeys_.assign(n, ID_EMPTY); tvals_.assign(n, NIL);
  tmask_ = n - 1; tcount_ = 0; ttomb_ = 0;
}
void Book::table_grow() {
  std::vector<u64> ok; std::vector<u32> ov;
  ok.swap(tkeys_); ov.swap(tvals_);
  size_t want = (tcount_ + 1) * 4;             // keep the load factor under ~0.5 after a rehash
  table_reset(want < 64 ? 64 : want);
  for (size_t i = 0; i < ok.size(); ++i)
    if (ok[i] != ID_EMPTY && ok[i] != ID_TOMB) { table_insert(ok[i], ov[i]); }
}
u32 Book::table_find(u64 id) const noexcept {
  if (tkeys_.empty()) return NIL;
  size_t h = (size_t)(hash_mix(0x2545f4914f6cdd1dull, id)) & tmask_;
  for (size_t i = 0;; ++i) {
    u64 k = tkeys_[h];
    if (k == id) return tvals_[h];
    if (k == ID_EMPTY) return NIL;
    h = (h + 1) & tmask_;
    if (i > tmask_) return NIL;
  }
}
bool Book::table_insert(u64 id, u32 slot) noexcept {
  if ((tcount_ + ttomb_ + 1) * 10 > tkeys_.size() * 7) table_grow();
  size_t h = (size_t)(hash_mix(0x2545f4914f6cdd1dull, id)) & tmask_;
  for (;;) {
    u64 k = tkeys_[h];
    if (k == ID_EMPTY || k == ID_TOMB) { if (k == ID_TOMB) --ttomb_; tkeys_[h] = id; tvals_[h] = slot; ++tcount_; return true; }
    if (k == id) { tvals_[h] = slot; return true; }
    h = (h + 1) & tmask_;
  }
}
void Book::table_erase(u64 id) noexcept {
  if (tkeys_.empty()) return;
  size_t h = (size_t)(hash_mix(0x2545f4914f6cdd1dull, id)) & tmask_;
  for (size_t i = 0;; ++i) {
    u64 k = tkeys_[h];
    if (k == id) { tkeys_[h] = ID_TOMB; tvals_[h] = NIL; --tcount_; ++ttomb_; return; }
    if (k == ID_EMPTY) return;
    h = (h + 1) & tmask_;
    if (i > tmask_) return;
  }
}

// --- intrusive FIFO
void Book::link(u32 s, bool buy, i64 tick) noexcept {
  Level& L = (buy ? bid_levels_ : ask_levels_)[(size_t)tick];
  Order& o = slab_[s];
  o.prev = L.tail; o.next = NIL;
  if (L.tail != NIL) slab_[L.tail].next = s; else L.head = s;
  L.tail = s; ++L.count;
}
void Book::unlink(u32 s) noexcept {
  Order& o = slab_[s];
  Level& L = (o.buy ? bid_levels_ : ask_levels_)[(size_t)o.tick];
  if (o.prev != NIL) slab_[o.prev].next = o.next; else L.head = o.next;
  if (o.next != NIL) slab_[o.next].prev = o.prev; else L.tail = o.prev;
  --L.count;
}

// --- add
AddResult Book::add(u64 id, u64 user, bool buy, i64 tick, u128 size, u32 flags) noexcept {
  AddResult r; fills_.clear();
  if (id == ID_EMPTY || id == ID_TOMB) { r.reason = R_BAD; ++rejects; hash_ = hash_mix(hash_mix(hash_, 0x12u), id); return r; }
  u32 dup = table_find(id);
  if (dup != NIL) { r.reason = R_DUPLICATE; r.remaining = slab_[dup].remaining; ++rejects; hash_ = hash_mix(hash_mix(hash_, 0x13u), id); return r; }
  if (tick <= 0 || size == 0) { r.reason = R_BAD; ++rejects; hash_ = hash_mix(hash_mix(hash_, 0x12u), id); return r; }
  if (!ensure_tick(tick)) { r.reason = R_DOMAIN; r.remaining = size; ++rejects; hash_ = hash_mix(hash_mix(hash_, 0x14u), id); return r; }

  r.seq = ++seq_; ++adds;
  hash_ = hash_mix(hash_mix(hash_mix(hash_mix(hash_mix(hash_mix(hash_, 1u), r.seq), id), user), (u64)tick),
                   ((u64)(size >> 64) ^ (u64)size) ^ ((u64)flags << 32) ^ (buy ? 1ull : 0ull));

  auto& far_levels = buy ? ask_levels_ : bid_levels_;
  auto& far_map = buy ? ask_map_ : bid_map_;
  const bool crosses_down = buy;   // a buy crosses asks at tick <= its own; a sell crosses bids at tick >= its own

  if (flags & F_POST_ONLY) {
    i64 bt = crosses_down ? far_map.min_set() : far_map.max_set();
    if (bt >= 0 && (crosses_down ? bt <= tick : bt >= tick)) { r.reason = R_WOULD_CROSS; r.remaining = size; ++rejects; return r; }
  }

  u128 rem = size;
  i64 t = crosses_down ? far_map.min_set() : far_map.max_set();
  while (t >= 0 && rem > 0 && (crosses_down ? t <= tick : t >= tick)) {
    Level& L = far_levels[(size_t)t];
    u32 s = L.head;
    while (s != NIL && rem > 0) {
      Order& o = slab_[s];
      u32 nx = o.next;
      u128 f = o.remaining < rem ? o.remaining : rem;          // strict price-time: the maker's own user is not special
      o.remaining -= f; rem -= f; L.total -= f;
      u64 fseq = ++seq_;
      fills_.push_back(Fill{fseq, o.id, id, o.user, user, t, f, o.remaining, buy});
      hash_ = hash_mix(hash_mix(hash_mix(hash_mix(hash_mix(hash_, 2u), fseq), o.id), id),
                       ((u64)(f >> 64) ^ (u64)f) ^ (u64)t);
      ++fills_total;
      if (o.remaining == 0) { unlink(s); table_erase(o.id); free_slot(s); --resting_; }
      s = nx;
    }
    bool emptied = (L.count == 0);
    if (emptied) { L.total = 0; far_map.clear((size_t)t); }
    if (rem == 0) break;
    t = emptied ? (crosses_down ? far_map.min_set() : far_map.max_set())
                : (crosses_down ? far_map.next_set((size_t)t + 1) : (t == 0 ? -1 : far_map.prev_set((size_t)t - 1)));
  }
  r.remaining = rem; r.nfills = (u32)fills_.size();

  if (rem > 0 && !(flags & F_IOC)) {
    Level& L = (buy ? bid_levels_ : ask_levels_)[(size_t)tick];
    if (L.total > (~(u128)0) - rem) { r.reason = R_OVERFLOW; ++rejects; return r; }   // checked: a level total never wraps
    u32 s = alloc_slot();
    if (s == NIL) { r.reason = R_FULL; ++rejects; return r; }
    Order& o = slab_[s];
    o.remaining = rem; o.size = size; o.id = id; o.user = user; o.seq = r.seq; o.tick = tick;
    o.prev = NIL; o.next = NIL; o.flags = flags; o.buy = buy; o.live = true;
    link(s, buy, tick);
    L.total += rem;
    (buy ? bid_map_ : ask_map_).set((size_t)tick);
    table_insert(id, s);
    ++resting_;
    r.rested = true;
  }
  return r;
}

CancelResult Book::cancel(u64 id) noexcept {
  CancelResult c;
  u32 s = table_find(id);
  if (s == NIL) return c;
  Order& o = slab_[s];
  Level& L = (o.buy ? bid_levels_ : ask_levels_)[(size_t)o.tick];
  L.total -= o.remaining;
  unlink(s);
  if (L.count == 0) { L.total = 0; (o.buy ? bid_map_ : ask_map_).clear((size_t)o.tick); }
  c.found = true; c.id = o.id; c.user = o.user; c.seq = o.seq; c.tick = o.tick;
  c.size = o.size; c.remaining = o.remaining; c.flags = o.flags; c.buy = o.buy;
  table_erase(id); free_slot(s); --resting_;
  ++seq_; ++cancels;
  hash_ = hash_mix(hash_mix(hash_mix(hash_, 3u), seq_), id);
  return c;
}

void Book::depth(int n, std::vector<LevelView>& bids, std::vector<LevelView>& asks) const {
  bids.clear(); asks.clear();
  if (n <= 0) return;
  for (i64 t = bid_map_.max_set(); t >= 0 && (int)bids.size() < n; t = (t == 0 ? -1 : bid_map_.prev_set((size_t)t - 1))) {
    const Level& L = bid_levels_[(size_t)t]; bids.push_back(LevelView{t, L.total, L.count});
  }
  for (i64 t = ask_map_.min_set(); t >= 0 && (int)asks.size() < n; t = ask_map_.next_set((size_t)t + 1)) {
    const Level& L = ask_levels_[(size_t)t]; asks.push_back(LevelView{t, L.total, L.count});
  }
}

bool Book::get(u64 id, OrderView& out) const noexcept {
  u32 s = table_find(id); if (s == NIL) return false;
  const Order& o = slab_[s];
  out = OrderView{o.id, o.user, o.seq, o.tick, o.size, o.remaining, o.flags, o.buy};
  return true;
}

size_t Book::orders(u64 user, bool filter, std::vector<OrderView>& out) const {
  out.clear();
  for (size_t i = 0; i < tkeys_.size(); ++i) {
    u64 k = tkeys_[i]; if (k == ID_EMPTY || k == ID_TOMB) continue;
    const Order& o = slab_[tvals_[i]];
    if (filter && o.user != user) continue;
    out.push_back(OrderView{o.id, o.user, o.seq, o.tick, o.size, o.remaining, o.flags, o.buy});
  }
  std::sort(out.begin(), out.end(), [](const OrderView& a, const OrderView& b) { return a.seq < b.seq; });
  return out.size();
}

/// acc (256 bits, limb 0 least significant) += a * b, modulo 2^256. Σ size·price overflows 128 bits on a
/// deep sweep (1e18-scaled size times a 1e18-scaled price is already 1e36 per fill), so the quote accumulator
/// is 256 bits wide and the caller divides by `filled` in its own big-integer arithmetic.
static inline void u256_addmul(u64 acc[4], u128 a, u128 b) noexcept {
  const u64 a0 = (u64)a, a1 = (u64)(a >> 64), b0 = (u64)b, b1 = (u64)(b >> 64);
  auto add_at = [&](int i, u128 v) noexcept {
    while (v != 0 && i < 4) { u128 s = (u128)acc[i] + (u64)v; acc[i] = (u64)s; v = (v >> 64) + (s >> 64); ++i; }
  };
  add_at(0, (u128)a0 * b0);
  add_at(1, (u128)a0 * b1);
  add_at(1, (u128)a1 * b0);
  add_at(2, (u128)a1 * b1);
}

PreviewResult Book::preview(bool buy, i64 tick, u128 size) const noexcept {
  PreviewResult p{}; p.filled = 0; p.fills = 0; for (int i = 0; i < 4; ++i) p.cost[i] = 0;
  u128 left = size;
  const auto& far_levels = buy ? ask_levels_ : bid_levels_;
  const auto& far_map = buy ? ask_map_ : bid_map_;
  i64 t = buy ? far_map.min_set() : far_map.max_set();
  while (t >= 0 && left > 0 && (buy ? t <= tick : t >= tick)) {
    const Level& L = far_levels[(size_t)t];
    for (u32 s = L.head; s != NIL && left > 0; s = slab_[s].next) {
      const Order& o = slab_[s];
      u128 f = o.remaining < left ? o.remaining : left;
      left -= f; ++p.fills;
      u256_addmul(p.cost, f, (u128)t * (u128)tick_size_);
    }
    t = buy ? far_map.next_set((size_t)t + 1) : (t == 0 ? -1 : far_map.prev_set((size_t)t - 1));
  }
  p.filled = size - left;
  return p;
}

u64 Book::book_hash() const noexcept {
  u64 h = 0xcbf29ce484222325ull;
  for (int side = 0; side < 2; ++side) {
    const bool buy = (side == 0);
    const auto& lvls = buy ? bid_levels_ : ask_levels_;
    const auto& map = buy ? bid_map_ : ask_map_;
    h = hash_mix(h, buy ? 0xB1Dull : 0xA5Bull);
    i64 t = buy ? map.max_set() : map.min_set();
    while (t >= 0) {
      const Level& L = lvls[(size_t)t];
      h = hash_mix(hash_mix(hash_mix(hash_mix(h, (u64)t), (u64)(L.total >> 64)), (u64)L.total), L.count);
      for (u32 s = L.head; s != NIL; s = slab_[s].next) {
        const Order& o = slab_[s];
        h = hash_mix(hash_mix(hash_mix(hash_mix(h, o.id), o.user), (u64)(o.remaining >> 64)), (u64)o.remaining);
      }
      t = buy ? (t == 0 ? -1 : map.prev_set((size_t)t - 1)) : map.next_set((size_t)t + 1);
    }
  }
  return h;
}

}  // namespace rollbook

// ------------------------------------------------------------------ C ABI
using rollbook::Book; using rollbook::u128; using rollbook::u64;
static inline u128 mk128(uint64_t lo, uint64_t hi) { return ((u128)hi << 64) | lo; }

extern "C" {

void* book_new(int64_t tick_size, int64_t tick_cap) {
  return new Book(tick_size, tick_cap ? tick_cap : rollbook::DEFAULT_TICK_CAP);
}
void* book_new_ex(int64_t tick_size, int64_t tick_cap, int64_t tick_limit, uint64_t slab_hint) {
  return new Book(tick_size, tick_cap ? tick_cap : rollbook::DEFAULT_TICK_CAP,
                  tick_limit ? tick_limit : rollbook::MAX_TICK_CAP - 1, (size_t)slab_hint);
}
void book_free(void* b) { delete (Book*)b; }

int32_t book_add(void* b, uint64_t id, uint64_t user, int32_t buy, int64_t tick, uint64_t size_lo, uint64_t size_hi, uint32_t flags, BookAddOut* out) {
  if (!b || !out) return -1;
  auto r = ((Book*)b)->add(id, user, buy != 0, tick, mk128(size_lo, size_hi), flags);
  out->seq = r.seq; out->rem_lo = (uint64_t)r.remaining; out->rem_hi = (uint64_t)(r.remaining >> 64);
  out->nfills = r.nfills; out->reason = (uint32_t)r.reason; out->rested = r.rested ? 1 : 0;
  return 0;
}
int32_t book_cancel(void* b, uint64_t id, BookOrderOut* out) {
  if (!b || !out) return -1;
  auto c = ((Book*)b)->cancel(id);
  out->found = c.found ? 1 : 0; out->id = c.id; out->user = c.user; out->seq = c.seq; out->tick = c.tick;
  out->size_lo = (uint64_t)c.size; out->size_hi = (uint64_t)(c.size >> 64);
  out->rem_lo = (uint64_t)c.remaining; out->rem_hi = (uint64_t)(c.remaining >> 64);
  out->flags = c.flags; out->buy = c.buy ? 1 : 0;
  return 0;
}
int32_t book_fills(void* b, BookFill* out, int32_t max) {
  if (!b || !out) return -1;
  const Book* bk = (const Book*)b;
  int32_t n = (int32_t)bk->fill_count(); if (n > max) n = max;
  const rollbook::Fill* f = bk->fills();
  for (int32_t i = 0; i < n; ++i) {
    out[i].seq = f[i].seq; out[i].maker_id = f[i].maker_id; out[i].taker_id = f[i].taker_id;
    out[i].maker_user = f[i].maker_user; out[i].taker_user = f[i].taker_user; out[i].tick = f[i].tick;
    out[i].size_lo = (uint64_t)f[i].size; out[i].size_hi = (uint64_t)(f[i].size >> 64);
    out[i].mrem_lo = (uint64_t)f[i].maker_remaining; out[i].mrem_hi = (uint64_t)(f[i].maker_remaining >> 64);
    out[i].taker_buys = f[i].taker_buys ? 1 : 0;
  }
  return n;
}
int32_t book_depth(void* b, int32_t n, BookLevelOut* bids, int32_t* nbids, BookLevelOut* asks, int32_t* nasks) {
  if (!b) return -1;
  std::vector<rollbook::LevelView> bv, av;
  ((Book*)b)->depth(n, bv, av);
  if (nbids) *nbids = (int32_t)bv.size();
  if (nasks) *nasks = (int32_t)av.size();
  auto cp = [](BookLevelOut* dst, const std::vector<rollbook::LevelView>& src) {
    for (size_t i = 0; i < src.size(); ++i) { dst[i].tick = src[i].tick; dst[i].total_lo = (uint64_t)src[i].total; dst[i].total_hi = (uint64_t)(src[i].total >> 64); dst[i].count = src[i].count; dst[i]._pad = 0; }
  };
  if (bids) cp(bids, bv);
  if (asks) cp(asks, av);
  return 0;
}
void book_best(void* b, int64_t* bid, int64_t* ask) {
  if (!b) return;
  if (bid) *bid = ((Book*)b)->best_bid();
  if (ask) *ask = ((Book*)b)->best_ask();
}
int32_t book_get(void* b, uint64_t id, BookOrderOut* out) {
  if (!b || !out) return -1;
  rollbook::OrderView o{};
  bool f = ((Book*)b)->get(id, o);
  out->found = f ? 1 : 0;
  if (f) { out->id = o.id; out->user = o.user; out->seq = o.seq; out->tick = o.tick;
           out->size_lo = (uint64_t)o.size; out->size_hi = (uint64_t)(o.size >> 64);
           out->rem_lo = (uint64_t)o.remaining; out->rem_hi = (uint64_t)(o.remaining >> 64);
           out->flags = o.flags; out->buy = o.buy ? 1 : 0; }
  return 0;
}
int32_t book_orders(void* b, uint64_t user, int32_t filter, BookOrderOut* out, int32_t max) {
  if (!b || !out) return -1;
  std::vector<rollbook::OrderView> v;
  ((Book*)b)->orders(user, filter != 0, v);
  int32_t n = (int32_t)v.size(); if (n > max) n = max;
  for (int32_t i = 0; i < n; ++i) {
    out[i].found = 1; out[i].id = v[i].id; out[i].user = v[i].user; out[i].seq = v[i].seq; out[i].tick = v[i].tick;
    out[i].size_lo = (uint64_t)v[i].size; out[i].size_hi = (uint64_t)(v[i].size >> 64);
    out[i].rem_lo = (uint64_t)v[i].remaining; out[i].rem_hi = (uint64_t)(v[i].remaining >> 64);
    out[i].flags = v[i].flags; out[i].buy = v[i].buy ? 1 : 0;
  }
  return (int32_t)v.size();
}
/// `user`/`has_user` are ignored: self-trades are allowed, so a quote includes the asker's own resting orders.
/// They stay in the signature because createBook().preview(buy, price, size, user) has them too.
int32_t book_preview(void* b, int32_t buy, int64_t tick, uint64_t size_lo, uint64_t size_hi, uint64_t user, int32_t has_user, BookPreviewOut* out) {
  if (!b || !out) return -1;
  (void)user; (void)has_user;
  auto p = ((Book*)b)->preview(buy != 0, tick, mk128(size_lo, size_hi));
  out->filled_lo = (uint64_t)p.filled; out->filled_hi = (uint64_t)(p.filled >> 64);
  for (int i = 0; i < 4; ++i) out->cost[i] = p.cost[i];
  out->fills = p.fills; out->_pad = 0;
  return 0;
}
uint64_t book_seq(void* b) { return b ? ((Book*)b)->seq() : 0; }
uint64_t book_size(void* b) { return b ? (uint64_t)((Book*)b)->size() : 0; }
uint64_t book_state_hash(void* b) { return b ? ((Book*)b)->state_hash() : 0; }
uint64_t book_book_hash(void* b) { return b ? ((Book*)b)->book_hash() : 0; }
int64_t book_tick_size(void* b) { return b ? ((Book*)b)->tick_size() : 0; }

}  // extern "C"
