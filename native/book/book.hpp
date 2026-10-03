// native/book/book.hpp — a deterministic price-time limit order book for ONE (market, outcome).
//
// Written against L3-SIDECHAIN.md §9: flat price-level arrays indexed by tick, intrusive FIFO per level,
// O(1) best bid/ask from a hierarchical bitmap, O(1) cancel by id, no allocation on the hot path, integer
// prices and 128-bit checked sizes. The matching rules are those of engine/l3/matcher.js, which is the
// reference oracle (diff.mjs replays the same workload against both): price-time priority, execution at the
// MAKER's price, FIFO inside a level, partial fills on both sides, no self-trade prevention (an order matches
// the same user's resting orders exactly like anyone else's — the owner's decision), post-only refused if it
// would cross, IOC never rests, a duplicate id (of a RESTING order) refused, and one
// monotonically increasing sequence number per book spent on every accepted order, every fill and every hit
// cancel — in that order, so two replicas that apply the same requests produce the same sequence numbers.
//
// Prices are int64 ticks: price_1e18 = tick * tick_size, tick_size configurable (default 1e14 = 0.0001, so
// outcome prices 0..1 live in ticks 0..10000). Sizes are unsigned 128-bit in the market's own units (the
// engine uses 1e18-scaled outcome tokens, which overflow int64 above ~9.2 tokens — hence 128 bits).
// Order ids and user ids are 64-bit: the caller maps order hashes and addresses onto them.
//
// Nothing here does I/O, takes a clock or throws on the hot path. bookd.cpp wraps it in a process with a
// journal; the extern "C" block at the bottom is the ABI other languages load.
#pragma once
#include <cstdint>
#include <cstddef>
#include <vector>

namespace rollbook {

using u64 = uint64_t; using i64 = int64_t; using u32 = uint32_t;
using u128 = unsigned __int128;

constexpr u32 NIL = 0xFFFFFFFFu;            // no slot
constexpr i64 NO_TICK = INT64_MIN;          // no best bid / no best ask
constexpr u64 ID_EMPTY = 0ull;              // reserved by the id table: rejected as an order id
constexpr u64 ID_TOMB  = ~0ull;             // reserved by the id table: rejected as an order id
constexpr i64 DEFAULT_TICK_SIZE = 100000000000000LL;   // 1e14
constexpr i64 DEFAULT_TICK_CAP  = 10001;               // outcome prices 0..1 at 1e14 → ticks 0..10000
constexpr i64 MAX_TICK_CAP      = 1LL << 24;           // the four-level bitmap addresses 64^4 ticks

enum Flags : u32 { F_NONE = 0, F_POST_ONLY = 1, F_IOC = 2 };

/// why an add did nothing. OK and WOULD_CROSS are the only ones matcher.js can produce ('duplicate',
/// 'bad order', 'would cross'); OFF_TICK/DOMAIN/OVERFLOW/FULL are native-only guards (see L3-NATIVE-BOOK.md).
enum Reason : u32 { R_OK = 0, R_DUPLICATE = 1, R_BAD = 2, R_WOULD_CROSS = 3, R_OFF_TICK = 4, R_DOMAIN = 5, R_OVERFLOW = 6, R_FULL = 7 };

struct Fill {
  u64 seq;                 // this book's sequence number for the fill
  u64 maker_id, taker_id;  // order ids
  u64 maker_user, taker_user;
  i64 tick;                // the MAKER's price
  u128 size;
  u128 maker_remaining;    // what is left of the maker's order after this fill (0 → it left the book)
  bool taker_buys;
};

struct AddResult {
  u64 seq = 0;             // the taker's own sequence number (0 if it was refused before being sequenced)
  u128 remaining = 0;
  u32 nfills = 0;
  bool rested = false;
  Reason reason = R_OK;
};

struct CancelResult {
  bool found = false;
  u64 id = 0, user = 0, seq = 0;
  i64 tick = 0;
  u128 size = 0, remaining = 0;
  u32 flags = 0;
  bool buy = false;
};

struct LevelView { i64 tick; u128 total; u32 count; };
struct OrderView { u64 id, user, seq; i64 tick; u128 size, remaining; u32 flags; bool buy; };
/// what a taker would get right now. `cost` is Σ size·price in the matcher's scale (1e18 size × 1e18 price →
/// 1e36), accumulated in 256 bits because 128 would overflow on a large sweep; the caller divides for the average.
struct PreviewResult { u128 filled; u64 cost[4]; u32 fills; };

/// a four-level hierarchical bitmap over ticks: one bit per tick, each higher level one bit per word below.
/// min/max set tick in four loads and four bit scans — the O(1) best bid / best ask §9 asks for.
class TickBitmap {
 public:
  void reset(size_t ticks);
  void set(size_t i) noexcept;
  void clear(size_t i) noexcept;
  i64 next_set(size_t from) const noexcept;   // lowest set tick >= from, or -1
  i64 prev_set(size_t from) const noexcept;   // highest set tick <= from, or -1
  i64 min_set() const noexcept { return empty() ? -1 : next_set(0); }
  i64 max_set() const noexcept { return empty() ? -1 : prev_set(cap_ - 1); }
  bool empty() const noexcept { return cap_ == 0 || lv_[4][0] == 0; }
  size_t cap() const noexcept { return cap_; }
 private:
  std::vector<u64> lv_[5];   // lv_[0] is one bit per tick; lv_[k] one bit per word of lv_[k-1]; lv_[4] is one word
  size_t bits_[5] = {0,0,0,0,0};   // logical element count per level
  size_t cap_ = 0;
};

class Book {
 public:
  /// tick_cap is the initial flat-array size; tick_limit is the highest tick this book will ever accept
  /// (pass 10000 for an outcome book so a price above 1.0 is refused instead of growing the domain).
  explicit Book(i64 tick_size = DEFAULT_TICK_SIZE, i64 tick_cap = DEFAULT_TICK_CAP,
                i64 tick_limit = MAX_TICK_CAP - 1, size_t slab_hint = 1024);

  /// a taker crosses the far side FIFO at the maker's price; what is left rests unless IOC. The fills of this
  /// call live in fills() until the next add(). noexcept: the only allocating paths (slab, id table and tick
  /// array growth) are taken outside steady state and abort rather than throw.
  AddResult add(u64 id, u64 user, bool buy, i64 tick, u128 size, u32 flags) noexcept;
  CancelResult cancel(u64 id) noexcept;

  const Fill* fills() const noexcept { return fills_.data(); }
  size_t fill_count() const noexcept { return fills_.size(); }

  void depth(int n, std::vector<LevelView>& bids, std::vector<LevelView>& asks) const;
  i64 best_bid() const noexcept { i64 t = bid_map_.max_set(); return t < 0 ? NO_TICK : t; }
  i64 best_ask() const noexcept { i64 t = ask_map_.min_set(); return t < 0 ? NO_TICK : t; }
  bool get(u64 id, OrderView& out) const noexcept;
  /// every resting order (of `user`, if filter), ordered by the sequence number it was accepted with
  size_t orders(u64 user, bool filter, std::vector<OrderView>& out) const;
  PreviewResult preview(bool buy, i64 tick, u128 size) const noexcept;

  u64 seq() const noexcept { return seq_; }
  size_t size() const noexcept { return resting_; }
  i64 tick_size() const noexcept { return tick_size_; }
  i64 tick_cap() const noexcept { return (i64)bid_levels_.size(); }
  i64 tick_limit() const noexcept { return tick_limit_; }

  /// a running hash over the request transcript (every add and cancel with its outcome, every fill, in order).
  /// Two replicas that applied the same requests in the same order hold the same value — the cheap equality
  /// check the miner log votes on. It is a *transcript* digest, not a commitment to the book's contents.
  u64 state_hash() const noexcept { return hash_; }
  /// a hash of the live book itself (levels ascending per side, orders in FIFO order). O(resting); for an
  /// epoch boundary, or to prove that two differently-arrived books are nonetheless identical.
  u64 book_hash() const noexcept;

  // counters, for the bench and for /v1/l3 status
  u64 adds = 0, fills_total = 0, cancels = 0, rejects = 0;

 private:
  struct Order {
    u128 remaining;  // 16
    u128 size;       // 16
    u64 id, user, seq;
    i64 tick;
    u32 prev, next;  // intrusive FIFO inside the level
    u32 flags;
    bool buy, live;
  };
  struct Level { u128 total = 0; u32 head = NIL, tail = NIL, count = 0; };

  // --- slab with a free list: no malloc on the hot path once warm
  u32 alloc_slot() noexcept;
  void free_slot(u32 s) noexcept;
  // --- id → slot, open addressing with linear probing and tombstones
  void table_reset(size_t slots);
  void table_grow();
  u32 table_find(u64 id) const noexcept;         // slot or NIL
  bool table_insert(u64 id, u32 slot) noexcept;
  void table_erase(u64 id) noexcept;
  // --- levels
  bool ensure_tick(i64 tick) noexcept;           // grow the tick domain if needed
  void link(u32 s, bool buy, i64 tick) noexcept;
  void unlink(u32 s) noexcept;

  i64 tick_size_, tick_limit_;
  std::vector<Level> bid_levels_, ask_levels_;
  TickBitmap bid_map_, ask_map_;
  std::vector<Order> slab_;
  u32 free_head_ = NIL;
  std::vector<u64> tkeys_;
  std::vector<u32> tvals_;
  size_t tcount_ = 0, ttomb_ = 0, tmask_ = 0;
  std::vector<Fill> fills_;
  u64 seq_ = 0, hash_ = 0xcbf29ce484222325ull;   // FNV-1a offset basis
  size_t resting_ = 0;
};

/// write `v` as decimal into buf (>= 40 bytes); returns the length. The fill lines the differential test
/// compares are 1e18-scaled integers, which do not fit 64 bits.
inline int u128_dec(u128 v, char* buf) noexcept {
  char tmp[40]; int n = 0;
  do { tmp[n++] = (char)('0' + (int)(v % 10)); v /= 10; } while (v);
  for (int i = 0; i < n; ++i) buf[i] = tmp[n - 1 - i];
  buf[n] = 0; return n;
}

/// the transcript mixer. Deterministic across compilers and platforms: no floats, no pointers, no padding.
inline u64 hash_mix(u64 h, u64 x) noexcept {
  h ^= x; h *= 0x100000001b3ull; h ^= h >> 29; h *= 0xbf58476d1ce4e5b9ull; return h;
}

}  // namespace rollbook

// ---------------------------------------------------------------------------------------------------
// C ABI: load libbook.so from Node (ffi/koffi), Rust, Python or Go. 128-bit values cross as lo/hi pairs,
// which every FFI understands; structs are POD with explicit padding.
extern "C" {

typedef struct { uint64_t seq, maker_id, taker_id, maker_user, taker_user; int64_t tick; uint64_t size_lo, size_hi, mrem_lo, mrem_hi; uint8_t taker_buys, _pad[7]; } BookFill;
typedef struct { uint64_t seq, rem_lo, rem_hi; uint32_t nfills, reason; uint8_t rested, _pad[7]; } BookAddOut;
typedef struct { uint64_t id, user, seq; int64_t tick; uint64_t size_lo, size_hi, rem_lo, rem_hi; uint32_t flags; uint8_t buy, found, _pad[2]; } BookOrderOut;
typedef struct { int64_t tick; uint64_t total_lo, total_hi; uint32_t count, _pad; } BookLevelOut;
typedef struct { uint64_t filled_lo, filled_hi, cost[4]; uint32_t fills, _pad; } BookPreviewOut;

void*    book_new(int64_t tick_size, int64_t tick_cap);
void*    book_new_ex(int64_t tick_size, int64_t tick_cap, int64_t tick_limit, uint64_t slab_hint);
void     book_free(void* b);
/// 0 on success (out->reason says what happened), -1 on a null handle
int32_t  book_add(void* b, uint64_t id, uint64_t user, int32_t buy, int64_t tick, uint64_t size_lo, uint64_t size_hi, uint32_t flags, BookAddOut* out);
int32_t  book_cancel(void* b, uint64_t id, BookOrderOut* out);
/// fills of the last book_add, up to max; returns how many were copied
int32_t  book_fills(void* b, BookFill* out, int32_t max);
/// top n levels per side; returns 0 and writes the counts
int32_t  book_depth(void* b, int32_t n, BookLevelOut* bids, int32_t* nbids, BookLevelOut* asks, int32_t* nasks);
void     book_best(void* b, int64_t* bid, int64_t* ask);
int32_t  book_get(void* b, uint64_t id, BookOrderOut* out);
int32_t  book_orders(void* b, uint64_t user, int32_t filter, BookOrderOut* out, int32_t max);
int32_t  book_preview(void* b, int32_t buy, int64_t tick, uint64_t size_lo, uint64_t size_hi, uint64_t user, int32_t has_user, BookPreviewOut* out);
uint64_t book_seq(void* b);
uint64_t book_size(void* b);
uint64_t book_state_hash(void* b);
uint64_t book_book_hash(void* b);
int64_t  book_tick_size(void* b);

}  // extern "C"
