#!/bin/bash
# Pull the miner's shared sources from the proto monorepo (the engine is the source of truth for the book semantics):
#   bash scripts/sync-from-proto.sh [/path/to/proto]
set -e; P=${1:-$HOME/new/proto}/rollmarkets; R=$(cd "$(dirname "$0")/.." && pwd)
cp $P/native/book/{book.hpp,book.cpp,bookd.cpp,test.cpp,trace.cpp,bench.cpp,Makefile,diff.mjs,bench.mjs} $R/native/book/
cp $P/engine/l3/matcher.js $R/src/matcher.js; cp $P/engine/l3/native.js $R/src/native.js; cp $P/engine/l3/matcher.test.mjs $P/engine/l3/native.test.mjs $R/src/
[ -d $P/engine/l3/miner ] && mkdir -p $R/src/miner && cp $P/engine/l3/miner/*.js $P/engine/l3/miner/*.mjs $R/src/miner/ 2>/dev/null || true
sed -i "s#'../../native/book/bookd'#'../native/book/bookd'#" $R/src/native.js
sed -i "s#cd ../../ \&\& \$(NODE) --test engine/l3/matcher.test.mjs engine/l3/native.test.mjs#cd ../../ \&\& \$(NODE) --test src/matcher.test.mjs src/native.test.mjs#" $R/native/book/Makefile
sed -i "s#'../../engine/l3/matcher.js'#'../../src/matcher.js'#; s#'../../engine/l3/native.js'#'../../src/native.js'#" $R/native/book/bench.mjs $R/native/book/diff.mjs
echo "synced from $P at $(git -C $P rev-parse --short HEAD)"
