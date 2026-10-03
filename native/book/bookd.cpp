// native/book/bookd.cpp — one book, one process, one journal: the per-market book process of L3-SIDECHAIN.md §3.
//
// Transport: length-prefixed binary frames (u32 little-endian payload length, then the payload) over a Unix
// domain socket (--listen), a pair of FIFOs (--fifo, which is what a synchronous Node client needs) or
// stdin/stdout (--stdio). Every reply carries the book's sequence number after the request was applied.
//
// Durability: every mutating request is appended to an append-only journal and fsynced BEFORE its ack. Requests
// that arrive together are journalled, fsynced once and only then applied and acked — group commit, so the
// fsync cost is amortised over the batch the sequencer sent, which is the §3 batch. On start the journal is
// replayed into the book: the same requests in the same order rebuild the same book, the same sequence numbers
// and the same transcript hash (book_state_hash), which is the property a standby or a validator replays with.
//
// Wire protocol (all integers little-endian; u128 as lo,hi):
//   request  := u8 op, body
//     1 ADD     u64 id, u64 user, u8 buy, u8 _r, u32 flags, i64 tick, u64 size_lo, u64 size_hi
//     2 CANCEL  u64 id
//     3 DEPTH   u32 n
//     4 BEST
//     5 ORDERS  u64 user, u8 filter
//     6 PREVIEW u8 buy, u8 _r (was has_user, ignored), i64 tick, u64 size_lo, u64 size_hi, u64 _r (was user)
//     7 GET     u64 id
//     8 STAT
//     9 SYNC                                    (fsync the journal and reply; for a clean handover)
//   reply    := u8 op, u8 status (0 ok, 1 bad request), u64 seq, body
//     1 ADD     u64 rem_lo, u64 rem_hi, u32 nfills, u8 rested, u8 reason,
//               nfills × (u64 seq, u64 maker_id, u64 taker_id, u64 maker_user, u64 taker_user, i64 tick,
//                         u64 size_lo, u64 size_hi, u64 maker_rem_lo, u64 maker_rem_hi, u8 taker_buys)
//     2 CANCEL  ORDER
//     3 DEPTH   u32 nbids, u32 nasks, levels × (i64 tick, u64 total_lo, u64 total_hi, u32 count)
//     4 BEST    i64 bid, i64 ask                 (INT64_MIN = none)
//     5 ORDERS  u32 n, n × ORDER
//     6 PREVIEW u64 filled_lo, u64 filled_hi, u64 cost[4], u32 fills
//     7 GET     ORDER
//     8 STAT    u64 resting, u64 state_hash, u64 book_hash, u64 journal_records, u64 adds, u64 fills,
//               u64 cancels, u64 rejects, i64 tick_size, i64 tick_limit
//     9 SYNC    u64 journal_records
//     ORDER     := u8 found, u64 id, u64 user, u64 seq, i64 tick, u64 size_lo, u64 size_hi, u64 rem_lo,
//                  u64 rem_hi, u32 flags, u8 buy
//
//   ./bookd --listen /tmp/book.sock [--journal path] [--fsync 0|1] [--tick-size N] [--tick-cap N] [--tick-limit N]
//   ./bookd --fifo req res [...]            ./bookd --stdio [...]
#include "book.hpp"
#include <sys/socket.h>
#include <sys/un.h>
#include <sys/stat.h>
#include <unistd.h>
#include <fcntl.h>
#include <poll.h>
#include <cerrno>
#include <csignal>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <vector>

using namespace rollbook;
using u8 = uint8_t;

enum Op : u8 { OP_ADD = 1, OP_CANCEL = 2, OP_DEPTH = 3, OP_BEST = 4, OP_ORDERS = 5, OP_PREVIEW = 6, OP_GET = 7, OP_STAT = 8, OP_SYNC = 9 };

// ---------------------------------------------------------------- little-endian framing
struct Rd {
  const u8* p; size_t n, i = 0; bool ok = true;
  u8 g8() { if (i + 1 > n) { ok = false; return 0; } return p[i++]; }
  u32 g32() { if (i + 4 > n) { ok = false; return 0; } u32 v = 0; for (int k = 0; k < 4; ++k) v |= (u32)p[i + k] << (8 * k); i += 4; return v; }
  u64 g64() { if (i + 8 > n) { ok = false; return 0; } u64 v = 0; for (int k = 0; k < 8; ++k) v |= (u64)p[i + k] << (8 * k); i += 8; return v; }
  i64 gi64() { return (i64)g64(); }
};
struct Wr {
  std::vector<u8>& b;
  void p8(u8 v) { b.push_back(v); }
  void p32(u32 v) { for (int k = 0; k < 4; ++k) b.push_back((u8)(v >> (8 * k))); }
  void p64(u64 v) { for (int k = 0; k < 8; ++k) b.push_back((u8)(v >> (8 * k))); }
  void pi64(i64 v) { p64((u64)v); }
  void p128(u128 v) { p64((u64)v); p64((u64)(v >> 64)); }
};

// ---------------------------------------------------------------- journal
struct Journal {
  int fd = -1;
  bool do_fsync = true;
  u64 records = 0;
  std::vector<u8> pend;
  std::string path;

  bool open_for_append(const std::string& p, i64 tick_size, i64 tick_limit) {
    path = p;
    fd = ::open(p.c_str(), O_WRONLY | O_CREAT | O_APPEND, 0644);
    if (fd < 0) { fprintf(stderr, "bookd: cannot open journal %s: %s\n", p.c_str(), strerror(errno)); return false; }
    off_t sz = lseek(fd, 0, SEEK_END);
    if (sz == 0) {                                      // a fresh journal starts with its parameters
      std::vector<u8> h; Wr w{h};
      const char* magic = "ROLLBOOK";
      for (int k = 0; k < 8; ++k) w.p8((u8)magic[k]);
      w.p32(1); w.pi64(tick_size); w.pi64(tick_limit); w.p32(0);
      if (write(fd, h.data(), h.size()) != (ssize_t)h.size()) return false;
      fsync(fd);
    }
    return true;
  }
  void append(const u8* payload, u32 len) {
    Wr w{pend}; w.p32(len);
    pend.insert(pend.end(), payload, payload + len);
    ++records;
  }
  /// durable before the ack: one write(2) and at most one fsync per arrival batch
  bool commit() {
    if (pend.empty() || fd < 0) { pend.clear(); return true; }
    size_t off = 0;
    while (off < pend.size()) {
      ssize_t w = write(fd, pend.data() + off, pend.size() - off);
      if (w < 0) { if (errno == EINTR) continue; fprintf(stderr, "bookd: journal write failed: %s\n", strerror(errno)); return false; }
      off += (size_t)w;
    }
    pend.clear();
    if (do_fsync && fdatasync(fd) < 0 && errno != EINVAL) { fprintf(stderr, "bookd: fdatasync failed: %s\n", strerror(errno)); return false; }
    return true;
  }
};

/// replay a journal into a fresh book: the same requests in the same order
static u64 replay(const std::string& path, Book& book, i64 tick_size, i64 tick_limit) {
  int fd = ::open(path.c_str(), O_RDONLY);
  if (fd < 0) return 0;
  std::vector<u8> all;
  {
    char buf[1 << 16]; ssize_t r;
    while ((r = read(fd, buf, sizeof buf)) > 0) all.insert(all.end(), buf, buf + r);
  }
  close(fd);
  size_t i = 0; u64 n = 0;
  if (all.size() >= 32 && !memcmp(all.data(), "ROLLBOOK", 8)) {
    Rd h{all.data() + 8, 24};
    u32 ver = h.g32(); i64 ts = h.gi64(), tl = h.gi64();
    if (ver != 1) { fprintf(stderr, "bookd: journal version %u, expected 1\n", ver); exit(3); }
    if (ts != tick_size || tl != tick_limit)
      { fprintf(stderr, "bookd: journal was written with tick size %lld limit %lld, started with %lld/%lld\n",
                (long long)ts, (long long)tl, (long long)tick_size, (long long)tick_limit); exit(3); }
    i = 32;
  }
  while (i + 4 <= all.size()) {
    u32 len = 0; for (int k = 0; k < 4; ++k) len |= (u32)all[i + k] << (8 * k);
    if (i + 4 + len > all.size()) { fprintf(stderr, "bookd: journal tail truncated at %zu, ignoring %zu bytes\n", i, all.size() - i); break; }
    Rd r{all.data() + i + 4, len};
    u8 op = r.g8();
    if (op == OP_ADD) {
      u64 id = r.g64(), user = r.g64(); u8 buy = r.g8(); r.g8(); u32 flags = r.g32(); i64 tick = r.gi64();
      u64 lo = r.g64(), hi = r.g64();
      if (r.ok) { book.add(id, user, buy != 0, tick, ((u128)hi << 64) | lo, flags); ++n; }
    } else if (op == OP_CANCEL) {
      u64 id = r.g64(); if (r.ok) { book.cancel(id); ++n; }
    }
    i += 4 + len;
  }
  return n;
}

// ---------------------------------------------------------------- request handling
struct Server {
  Book book;
  Journal jrn;
  Server(i64 ts, i64 cap, i64 lim) : book(ts, cap, lim, 1u << 16) {}

  static void put_order(Wr& w, bool found, u64 id, u64 user, u64 seq, i64 tick, u128 size, u128 rem, u32 flags, bool buy) {
    w.p8(found ? 1 : 0); w.p64(id); w.p64(user); w.p64(seq); w.pi64(tick); w.p128(size); w.p128(rem); w.p32(flags); w.p8(buy ? 1 : 0);
  }

  /// write one reply frame for one request payload into `out`
  void handle(const u8* payload, u32 len, std::vector<u8>& out) {
    size_t at = out.size();
    Wr w{out}; w.p32(0);                                  // length patched below
    Rd r{payload, len};
    u8 op = r.g8();
    w.p8(op); w.p8(0); size_t seq_at = out.size(); w.p64(0);   // seq patched after the request is applied
    switch (op) {
      case OP_ADD: {
        u64 id = r.g64(), user = r.g64(); u8 buy = r.g8(); r.g8(); u32 flags = r.g32(); i64 tick = r.gi64();
        u64 lo = r.g64(), hi = r.g64();
        if (!r.ok) { out[at + 4 + 1] = 1; break; }
        auto res = book.add(id, user, buy != 0, tick, ((u128)hi << 64) | lo, flags);
        w.p128(res.remaining); w.p32(res.nfills); w.p8(res.rested ? 1 : 0); w.p8((u8)res.reason);
        const Fill* f = book.fills();
        for (size_t k = 0; k < book.fill_count(); ++k) {
          w.p64(f[k].seq); w.p64(f[k].maker_id); w.p64(f[k].taker_id); w.p64(f[k].maker_user); w.p64(f[k].taker_user);
          w.pi64(f[k].tick); w.p128(f[k].size); w.p128(f[k].maker_remaining); w.p8(f[k].taker_buys ? 1 : 0);
        }
        break;
      }
      case OP_CANCEL: {
        u64 id = r.g64(); if (!r.ok) { out[at + 4 + 1] = 1; break; }
        auto c = book.cancel(id);
        put_order(w, c.found, c.id, c.user, c.seq, c.tick, c.size, c.remaining, c.flags, c.buy);
        break;
      }
      case OP_DEPTH: {
        u32 n = r.g32(); if (!r.ok) { out[at + 4 + 1] = 1; break; }
        std::vector<LevelView> bids, asks; book.depth((int)n, bids, asks);
        w.p32((u32)bids.size()); w.p32((u32)asks.size());
        for (auto& l : bids) { w.pi64(l.tick); w.p128(l.total); w.p32(l.count); }
        for (auto& l : asks) { w.pi64(l.tick); w.p128(l.total); w.p32(l.count); }
        break;
      }
      case OP_BEST: w.pi64(book.best_bid()); w.pi64(book.best_ask()); break;
      case OP_ORDERS: {
        u64 user = r.g64(); u8 filter = r.g8(); if (!r.ok) { out[at + 4 + 1] = 1; break; }
        std::vector<OrderView> v; book.orders(user, filter != 0, v);
        w.p32((u32)v.size());
        for (auto& o : v) put_order(w, true, o.id, o.user, o.seq, o.tick, o.size, o.remaining, o.flags, o.buy);
        break;
      }
      case OP_PREVIEW: {
        u8 buy = r.g8(); r.g8(); i64 tick = r.gi64(); u64 lo = r.g64(), hi = r.g64(); r.g64();
        if (!r.ok) { out[at + 4 + 1] = 1; break; }
        auto p = book.preview(buy != 0, tick, ((u128)hi << 64) | lo);   // has_user/user ignored: self-trades are allowed
        w.p128(p.filled); for (int k = 0; k < 4; ++k) w.p64(p.cost[k]); w.p32(p.fills);
        break;
      }
      case OP_GET: {
        u64 id = r.g64(); if (!r.ok) { out[at + 4 + 1] = 1; break; }
        OrderView o{}; bool f = book.get(id, o);
        put_order(w, f, o.id, o.user, o.seq, o.tick, o.size, o.remaining, o.flags, o.buy);
        break;
      }
      case OP_STAT:
        w.p64((u64)book.size()); w.p64(book.state_hash()); w.p64(book.book_hash()); w.p64(jrn.records);
        w.p64(book.adds); w.p64(book.fills_total); w.p64(book.cancels); w.p64(book.rejects);
        w.pi64(book.tick_size()); w.pi64(book.tick_limit());
        break;
      case OP_SYNC: w.p64(jrn.records); break;
      default: out[at + 4 + 1] = 1; break;
    }
    u64 s = book.seq();
    for (int k = 0; k < 8; ++k) out[seq_at + k] = (u8)(s >> (8 * k));
    u32 plen = (u32)(out.size() - at - 4);
    for (int k = 0; k < 4; ++k) out[at + k] = (u8)(plen >> (8 * k));
  }
};

// ---------------------------------------------------------------- the loop
static bool write_all(int fd, const u8* p, size_t n) {
  size_t off = 0;
  while (off < n) {
    ssize_t w = write(fd, p + off, n - off);
    if (w < 0) { if (errno == EINTR) continue; if (errno == EAGAIN) { struct pollfd pf{fd, POLLOUT, 0}; poll(&pf, 1, 1000); continue; } return false; }
    off += (size_t)w;
  }
  return true;
}

/// serve one connection until EOF. Requests that arrive together are journalled, fsynced once, then applied.
static void serve(Server& srv, int rfd, int wfd, bool quiet) {
  std::vector<u8> in, out; in.reserve(1 << 16); out.reserve(1 << 16);
  u8 buf[1 << 16];
  u64 nreq = 0, nbatch = 0;
  for (;;) {
    struct pollfd pf{rfd, POLLIN, 0};
    int pr = poll(&pf, 1, -1);
    if (pr < 0) { if (errno == EINTR) continue; break; }
    ssize_t n = read(rfd, buf, sizeof buf);
    if (n == 0) break;
    if (n < 0) { if (errno == EINTR || errno == EAGAIN) continue; break; }
    in.insert(in.end(), buf, buf + n);
    // one pass to find the complete frames that arrived, journal the mutating ones, fsync once
    std::vector<std::pair<size_t, u32>> frames;
    size_t i = 0;
    while (i + 4 <= in.size()) {
      u32 len = 0; for (int k = 0; k < 4; ++k) len |= (u32)in[i + k] << (8 * k);
      if (len == 0 || len > (1u << 20) || i + 4 + len > in.size()) break;
      frames.push_back({i + 4, len});
      i += 4 + len;
    }
    if (frames.empty()) { if (i == 0 && in.size() > (1u << 21)) break; continue; }
    for (auto& fr : frames) { u8 op = in[fr.first]; if (op == OP_ADD || op == OP_CANCEL) srv.jrn.append(in.data() + fr.first, fr.second); }
    if (!srv.jrn.commit()) { fprintf(stderr, "bookd: journal commit failed, refusing to ack\n"); exit(4); }
    out.clear();
    for (auto& fr : frames) srv.handle(in.data() + fr.first, fr.second, out);
    nreq += frames.size(); ++nbatch;
    in.erase(in.begin(), in.begin() + (long)i);
    if (!write_all(wfd, out.data(), out.size())) break;
  }
  if (!quiet)
    fprintf(stderr, "bookd: connection closed · %llu requests in %llu batches (%.1f per batch) · seq %llu · resting %zu · transcript %016llx\n",
            (unsigned long long)nreq, (unsigned long long)nbatch, nbatch ? (double)nreq / (double)nbatch : 0.0,
            (unsigned long long)srv.book.seq(), srv.book.size(), (unsigned long long)srv.book.state_hash());
}

int main(int argc, char** argv) {
  signal(SIGPIPE, SIG_IGN);
  std::string sock, fifo_req, fifo_res, jpath = "book.journal";
  bool stdio = false, quiet = false, nojournal = false, do_fsync = true;
  i64 tick_size = DEFAULT_TICK_SIZE, tick_cap = DEFAULT_TICK_CAP, tick_limit = DEFAULT_TICK_CAP - 1;
  for (int i = 1; i < argc; ++i) {
    std::string a = argv[i];
    auto next = [&]() -> std::string { return i + 1 < argc ? argv[++i] : std::string(); };
    if (a == "--listen") sock = next();
    else if (a == "--fifo") { fifo_req = next(); fifo_res = next(); }
    else if (a == "--stdio") stdio = true;
    else if (a == "--journal") { std::string p = next(); if (p == "none") nojournal = true; else jpath = p; }
    else if (a == "--fsync") do_fsync = next() != "0";
    else if (a == "--tick-size") tick_size = atoll(next().c_str());
    else if (a == "--tick-cap") tick_cap = atoll(next().c_str());
    else if (a == "--tick-limit") tick_limit = atoll(next().c_str());
    else if (a == "--quiet") quiet = true;
    else { fprintf(stderr, "bookd: unknown argument %s\n", a.c_str()); return 2; }
  }
  if (sock.empty() && fifo_req.empty() && !stdio) { fprintf(stderr, "bookd: one of --listen, --fifo or --stdio is required\n"); return 2; }

  Server srv(tick_size, tick_cap, tick_limit);
  srv.jrn.do_fsync = do_fsync;
  if (!nojournal) {
    u64 n = replay(jpath, srv.book, tick_size, tick_limit);
    if (!srv.jrn.open_for_append(jpath, tick_size, tick_limit)) return 3;
    srv.jrn.records = n;
    if (!quiet)
      fprintf(stderr, "bookd: replayed %llu journal records from %s · seq %llu · resting %zu · transcript %016llx\n",
              (unsigned long long)n, jpath.c_str(), (unsigned long long)srv.book.seq(), srv.book.size(),
              (unsigned long long)srv.book.state_hash());
  } else if (!quiet) fprintf(stderr, "bookd: NO JOURNAL (--journal none): acks are not durable\n");
  if (!quiet && !do_fsync) fprintf(stderr, "bookd: fsync off: an ack survives a crash of this process but not of the machine\n");

  if (stdio) { serve(srv, 0, 1, quiet); return 0; }
  if (!fifo_req.empty()) {
    int rfd = ::open(fifo_req.c_str(), O_RDONLY);
    if (rfd < 0) { fprintf(stderr, "bookd: cannot open %s: %s\n", fifo_req.c_str(), strerror(errno)); return 3; }
    int wfd = ::open(fifo_res.c_str(), O_WRONLY);
    if (wfd < 0) { fprintf(stderr, "bookd: cannot open %s: %s\n", fifo_res.c_str(), strerror(errno)); return 3; }
    if (!quiet) fprintf(stderr, "bookd: serving on fifos %s → %s\n", fifo_req.c_str(), fifo_res.c_str());
    serve(srv, rfd, wfd, quiet);
    return 0;
  }
  // Unix domain socket: one book process, served to one client at a time (§3 is one book process per market)
  unlink(sock.c_str());
  int ls = socket(AF_UNIX, SOCK_STREAM, 0);
  if (ls < 0) { perror("socket"); return 3; }
  struct sockaddr_un addr{}; addr.sun_family = AF_UNIX;
  if (sock.size() >= sizeof addr.sun_path) { fprintf(stderr, "bookd: socket path too long\n"); return 2; }
  strncpy(addr.sun_path, sock.c_str(), sizeof addr.sun_path - 1);
  if (bind(ls, (struct sockaddr*)&addr, sizeof addr) < 0) { perror("bind"); return 3; }
  if (listen(ls, 8) < 0) { perror("listen"); return 3; }
  if (!quiet) fprintf(stderr, "bookd: listening on %s\n", sock.c_str());
  for (;;) {
    int cs = accept(ls, nullptr, nullptr);
    if (cs < 0) { if (errno == EINTR) continue; perror("accept"); break; }
    serve(srv, cs, cs, quiet);
    close(cs);
  }
  unlink(sock.c_str());
  return 0;
}
