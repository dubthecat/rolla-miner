// native/book/commit_trace.cpp — drive the native commit tree from a line protocol on stdin, so commit_diff.mjs
// can feed the same random operations to it and to engine/l3/miner/commit.js and compare the roots.
//
//   i <hash:64hex> <user:40hex> <buy:0|1> <price:64hex> <remaining:64hex> <seq:decimal>     insert
//   r <hash>                                                                               remove
//   u <hash> <remaining:64hex>                                                             set_remaining
//   ?                   → "root <64hex> size <n> acc <accepted-op-count>"
//   p <hash>            → "proof <hex of the wire form>" or "proof -"
// Anything else is an error (exit 2). Output is flushed at the end; stdin is read to EOF.
#include "commit.hpp"
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <vector>

using namespace rollcommit;
static const char* HEXD = "0123456789abcdef";
static bool unhex(const char* s, size_t n, u8* out) {
  for (size_t i = 0; i < n; ++i) {
    int v = 0;
    for (int k = 0; k < 2; ++k) { char c = s[2 * i + k]; int d = c >= '0' && c <= '9' ? c - '0' : c >= 'a' && c <= 'f' ? c - 'a' + 10 : c >= 'A' && c <= 'F' ? c - 'A' + 10 : -1; if (d < 0) return false; v = v * 16 + d; }
    out[i] = (u8)v;
  }
  return true;
}
static void hexout(std::string& o, const u8* p, size_t n) { for (size_t i = 0; i < n; ++i) { o += HEXD[p[i] >> 4]; o += HEXD[p[i] & 15]; } }

int main() {
  CommitTree t(1 << 16);
  char line[1024]; std::string out; out.reserve(1 << 20);
  u64 acc = 0; long lineno = 0;
  std::vector<u8> pbuf;
  while (fgets(line, sizeof line, stdin)) {
    ++lineno;
    char* f[8]; int nf = 0;
    for (char* p = strtok(line, " \t\r\n"); p && nf < 8; p = strtok(nullptr, " \t\r\n")) f[nf++] = p;
    if (!nf) continue;
    bool ok = true;
    switch (f[0][0]) {
      case 'i': {
        Entry e; ok = nf == 7 && strlen(f[1]) == 64 && strlen(f[2]) == 40 && strlen(f[4]) == 64 && strlen(f[5]) == 64
                  && unhex(f[1], 32, e.hash) && unhex(f[2], 20, e.user) && unhex(f[4], 32, e.price) && unhex(f[5], 32, e.remaining);
        if (!ok) break;
        e.buy = f[3][0] == '1'; e.seq = strtoull(f[6], nullptr, 10);
        if (t.insert(e)) ++acc;
        break;
      }
      case 'r': { u8 h[32]; ok = nf == 2 && strlen(f[1]) == 64 && unhex(f[1], 32, h); if (ok && t.remove(h)) ++acc; break; }
      case 'u': { u8 h[32], rem[32]; ok = nf == 3 && strlen(f[1]) == 64 && strlen(f[2]) == 64 && unhex(f[1], 32, h) && unhex(f[2], 32, rem); if (ok && t.set_remaining(h, rem)) ++acc; break; }
      case '?': { u8 r[32]; t.root(r); out += "root "; hexout(out, r, 32); out += " size " + std::to_string(t.size()) + " acc " + std::to_string(acc) + "\n"; break; }
      case 'p': {
        u8 h[32]; Proof p; ok = nf == 2 && strlen(f[1]) == 64 && unhex(f[1], 32, h);
        if (!ok) break;
        if (!t.proof(h, p)) { out += "proof -\n"; break; }
        pbuf.resize(proof_bytes(p)); proof_write(p, pbuf.data());
        out += "proof "; hexout(out, pbuf.data(), pbuf.size()); out += "\n";
        break;
      }
      default: ok = false;
    }
    if (!ok) { fprintf(stderr, "commit_trace: bad line %ld: %s\n", lineno, f[0]); return 2; }
    if (out.size() > (1u << 20)) { fwrite(out.data(), 1, out.size(), stdout); out.clear(); }
  }
  if (!out.empty()) fwrite(out.data(), 1, out.size(), stdout);
  return 0;
}
