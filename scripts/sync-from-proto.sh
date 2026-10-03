#!/bin/bash
# Pull the miner's shared sources from the proto monorepo (the engine is the source of truth for the book semantics):
#   bash scripts/sync-from-proto.sh [/path/to/proto]
set -e; P=${1:-$HOME/new/proto}/rollmarkets; R=$(cd "$(dirname "$0")/.." && pwd)
cp $P/native/book/{book.hpp,book.cpp,bookd.cpp,test.cpp,trace.cpp,bench.cpp,Makefile,diff.mjs,bench.mjs} $R/native/book/
for f in $P/native/book/commit* $P/native/book/keccak*; do [ -e "$f" ] && cp "$f" $R/native/book/; done   # the incremental state commitment, once it exists
cp $P/engine/l3/matcher.js $R/src/matcher.js; cp $P/engine/l3/native.js $R/src/native.js; cp $P/engine/l3/matcher.test.mjs $P/engine/l3/native.test.mjs $R/src/
mkdir -p $R/src/miner && cp $P/engine/l3/miner/*.js $P/engine/l3/miner/*.mjs $R/src/miner/ && cp $P/lib/desk.js $R/src/desk.js && sed -i "s#'../../../lib/desk.js'#'../desk.js'#g" $R/src/miner/*.js $R/src/miner/*.mjs
sed -i "s#'../../native/book/bookd'#'../native/book/bookd'#" $R/src/native.js
sed -i "s#cd ../../ \&\& \$(NODE) --test engine/l3/matcher.test.mjs engine/l3/native.test.mjs#cd ../../ \&\& \$(NODE) --test src/matcher.test.mjs src/native.test.mjs#" $R/native/book/Makefile
sed -i "s#'../../engine/l3/matcher.js'#'../../src/matcher.js'#; s#'../../engine/l3/native.js'#'../../src/native.js'#" $R/native/book/bench.mjs $R/native/book/diff.mjs
echo "synced from $P at $(git -C $P rev-parse --short HEAD)"
