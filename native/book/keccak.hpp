// native/book/keccak.hpp — Keccak-256, header-only, no dependencies. The ETHEREUM variant: the original Keccak
// padding (0x01 … 0x80), not SHA3-256's 0x06 — so keccak256("") = c5d2…a470, the value every EVM contract and
// merkle.js's `keccak` binding produce. keccak-f[1600] over 25 little-endian 64-bit lanes, rate 136 bytes.
//
// Written for this repository from the Keccak specification (the round constants, rotation offsets and lane
// permutation are the standard's); nothing is borrowed. The permutation is the plain four-step form —
// theta, rho+pi, chi, iota — with the lane indices written out (see f1600 for why). A 97-byte node encoding
// is one permutation, a 193-byte leaf two: the number of permutations per tree operation is the cost.
//
//   keccak::hash256(in, n, out32)                       one shot
//   keccak::Hasher h; h.update(p, n); h.finish(out32);  incremental, for the tagged encodings
//
// commit_test.cpp pins it to the published vectors for "" and "abc", to a two-block message, and to a
// restingLeaf computed by merkle.js; commit_diff.mjs compares every root with the JavaScript tree.
#pragma once
#include <cstdint>
#include <cstddef>
#include <cstring>

namespace keccak {

constexpr size_t RATE = 136;   // (1600 - 2·256) / 8 bytes per block

namespace detail {
constexpr uint64_t RC[24] = {
  0x0000000000000001ull, 0x0000000000008082ull, 0x800000000000808aull, 0x8000000080008000ull,
  0x000000000000808bull, 0x0000000080000001ull, 0x8000000080008081ull, 0x8000000000008009ull,
  0x000000000000008aull, 0x0000000000000088ull, 0x0000000080008009ull, 0x000000008000000aull,
  0x000000008000808bull, 0x800000000000008bull, 0x8000000000008089ull, 0x8000000000008003ull,
  0x8000000000008002ull, 0x8000000000000080ull, 0x000000000000800aull, 0x800000008000000aull,
  0x8000000080008081ull, 0x8000000000008080ull, 0x0000000080000001ull, 0x8000000080008008ull,
};
inline uint64_t rotl(uint64_t x, int n) noexcept { return (x << n) | (x >> (64 - n)); }

// One round, with every lane index written out: g++ 11 at -O2 does not unroll the five-wide loops of the
// textbook form (the `% 5` indexing survives), and that made a permutation 3.6 µs instead of ~0.3 µs — ten
// times the cost of every tree operation. The lane map is the standard one, (x, y) at index x + 5y:
// rho rotates lane (x, y) by r[x][y], pi moves it to (y, 2x + 3y), chi mixes each row, iota adds the constant.
inline void f1600(uint64_t a[25]) noexcept {
  uint64_t b[25], c0, c1, c2, c3, c4, d;
  for (int r = 0; r < 24; ++r) {
    // theta
    c0 = a[0] ^ a[5] ^ a[10] ^ a[15] ^ a[20];
    c1 = a[1] ^ a[6] ^ a[11] ^ a[16] ^ a[21];
    c2 = a[2] ^ a[7] ^ a[12] ^ a[17] ^ a[22];
    c3 = a[3] ^ a[8] ^ a[13] ^ a[18] ^ a[23];
    c4 = a[4] ^ a[9] ^ a[14] ^ a[19] ^ a[24];
    d = c4 ^ rotl(c1, 1); a[0] ^= d; a[5] ^= d; a[10] ^= d; a[15] ^= d; a[20] ^= d;
    d = c0 ^ rotl(c2, 1); a[1] ^= d; a[6] ^= d; a[11] ^= d; a[16] ^= d; a[21] ^= d;
    d = c1 ^ rotl(c3, 1); a[2] ^= d; a[7] ^= d; a[12] ^= d; a[17] ^= d; a[22] ^= d;
    d = c2 ^ rotl(c4, 1); a[3] ^= d; a[8] ^= d; a[13] ^= d; a[18] ^= d; a[23] ^= d;
    d = c3 ^ rotl(c0, 1); a[4] ^= d; a[9] ^= d; a[14] ^= d; a[19] ^= d; a[24] ^= d;
    // rho + pi: b[y + 5·((2x + 3y) mod 5)] = rotl(a[x + 5y], r[x][y])
    b[0]  = a[0];
    b[10] = rotl(a[1], 1);   b[20] = rotl(a[2], 62);  b[5]  = rotl(a[3], 28);  b[15] = rotl(a[4], 27);
    b[16] = rotl(a[5], 36);  b[1]  = rotl(a[6], 44);  b[11] = rotl(a[7], 6);   b[21] = rotl(a[8], 55);  b[6]  = rotl(a[9], 20);
    b[7]  = rotl(a[10], 3);  b[17] = rotl(a[11], 10); b[2]  = rotl(a[12], 43); b[12] = rotl(a[13], 25); b[22] = rotl(a[14], 39);
    b[23] = rotl(a[15], 41); b[8]  = rotl(a[16], 45); b[18] = rotl(a[17], 15); b[3]  = rotl(a[18], 21); b[13] = rotl(a[19], 8);
    b[14] = rotl(a[20], 18); b[24] = rotl(a[21], 2);  b[9]  = rotl(a[22], 61); b[19] = rotl(a[23], 56); b[4]  = rotl(a[24], 14);
    // chi, row by row: a[x] = b[x] ^ (~b[x+1] & b[x+2])
    for (int j = 0; j < 25; j += 5) {
      a[j]     = b[j]     ^ (~b[j + 1] & b[j + 2]);
      a[j + 1] = b[j + 1] ^ (~b[j + 2] & b[j + 3]);
      a[j + 2] = b[j + 2] ^ (~b[j + 3] & b[j + 4]);
      a[j + 3] = b[j + 3] ^ (~b[j + 4] & b[j]);
      a[j + 4] = b[j + 4] ^ (~b[j]     & b[j + 1]);
    }
    // iota
    a[0] ^= RC[r];
  }
}
inline uint64_t load64(const uint8_t* p) noexcept {
  uint64_t v = 0;
  for (int i = 7; i >= 0; --i) v = (v << 8) | p[i];
  return v;
}
inline void store64(uint8_t* p, uint64_t v) noexcept { for (int i = 0; i < 8; ++i) { p[i] = (uint8_t)v; v >>= 8; } }
}  // namespace detail

class Hasher {
 public:
  Hasher() noexcept { reset(); }
  void reset() noexcept { std::memset(st_, 0, sizeof st_); pos_ = 0; }
  void update(const uint8_t* p, size_t n) noexcept {
    while (n) {
      size_t take = RATE - pos_; if (take > n) take = n;
      std::memcpy(buf_ + pos_, p, take); pos_ += take; p += take; n -= take;
      if (pos_ == RATE) { absorb(); pos_ = 0; }
    }
  }
  /// pad and squeeze 32 bytes; the hasher is reset afterwards so it can be reused
  void finish(uint8_t out[32]) noexcept {
    std::memset(buf_ + pos_, 0, RATE - pos_);
    buf_[pos_] ^= 0x01;             // Keccak's pad10*1: the 0x01 domain byte …
    buf_[RATE - 1] ^= 0x80;         // … and the final 1 bit, which may share the same byte
    absorb();
    for (int i = 0; i < 4; ++i) detail::store64(out + 8 * i, st_[i]);
    reset();
  }
 private:
  void absorb() noexcept {
    for (size_t i = 0; i < RATE / 8; ++i) st_[i] ^= detail::load64(buf_ + 8 * i);
    detail::f1600(st_);
  }
  uint64_t st_[25];
  uint8_t buf_[RATE];
  size_t pos_;
};

inline void hash256(const uint8_t* in, size_t n, uint8_t out[32]) noexcept { Hasher h; h.update(in, n); h.finish(out); }

}  // namespace keccak
