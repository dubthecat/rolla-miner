# native/book/Makefile — the L3 native order book. g++ 11 and C++20; no external dependencies.
#   make            build everything with -O2
#   make test       the C++ unit tests, the Node unit tests, and the differential check against matcher.js
#   make bench      the 1M mixed workload, C++ (-O2 and -O3 -march=native) and Node, side by side
#   make commit-bench  the commit tree: 1M inserts, 500k fills, 500k removes in C++ (-O2, -O3) and Node
#   make bench10    the 10M mixed workload and the pathological 100k-deep level
#   make asan       the tests and the differential check under ASan + UBSan
#   make clean
CXX      ?= g++
STD      := -std=c++20
WARN     := -Wall -Wextra
OPT2     := -O2
OPT3     := -O3 -march=native
LDLIBS   :=
SRC      := book.cpp
HDR      := book.hpp
CSRC     := commit.cpp
CHDR     := commit.hpp keccak.hpp
NODE     ?= node

BINS := book_test trace bench bookd trace3 bench3 libbook.so commit_test commit_trace commit_bench commit_bench3

all: $(BINS)

book_test: test.cpp $(SRC) $(HDR)
	$(CXX) $(STD) $(OPT2) $(WARN) test.cpp $(SRC) -o $@ $(LDLIBS)

trace: trace.cpp $(SRC) $(HDR)
	$(CXX) $(STD) $(OPT2) $(WARN) trace.cpp $(SRC) -o $@ $(LDLIBS)

trace3: trace.cpp $(SRC) $(HDR)
	$(CXX) $(STD) $(OPT3) $(WARN) trace.cpp $(SRC) -o $@ $(LDLIBS)

bench: bench.cpp $(SRC) $(HDR)
	$(CXX) $(STD) $(OPT2) $(WARN) bench.cpp $(SRC) -o $@ $(LDLIBS)

bench3: bench.cpp $(SRC) $(HDR)
	$(CXX) $(STD) $(OPT3) $(WARN) bench.cpp $(SRC) -o $@ $(LDLIBS)

bookd: bookd.cpp $(SRC) $(HDR)
	$(CXX) $(STD) $(OPT2) $(WARN) bookd.cpp $(SRC) -o $@ $(LDLIBS)

# the incremental state commitment (commit.hpp): the Merkle-treap over the resting book, and its keccak
commit_test: commit_test.cpp $(CSRC) $(CHDR)
	$(CXX) $(STD) $(OPT2) $(WARN) commit_test.cpp $(CSRC) -o $@ $(LDLIBS)

commit_trace: commit_trace.cpp $(CSRC) $(CHDR)
	$(CXX) $(STD) $(OPT2) $(WARN) commit_trace.cpp $(CSRC) -o $@ $(LDLIBS)

commit_bench: commit_bench.cpp $(CSRC) $(CHDR)
	$(CXX) $(STD) $(OPT2) $(WARN) commit_bench.cpp $(CSRC) -o $@ $(LDLIBS)

commit_bench3: commit_bench.cpp $(CSRC) $(CHDR)
	$(CXX) $(STD) $(OPT3) $(WARN) commit_bench.cpp $(CSRC) -o $@ $(LDLIBS)

# the C ABI as a shared object, for Node (koffi/ffi), Rust, Python or Go: the book and the commit tree
libbook.so: $(SRC) $(CSRC) $(HDR) $(CHDR)
	$(CXX) $(STD) $(OPT2) $(WARN) -fPIC -shared $(SRC) $(CSRC) -o $@

test: book_test trace bookd commit_test commit_trace
	@echo "== C++ unit tests"
	./book_test
	@echo "== Node unit tests (the reference book and the native client)"
	cd ../../ && $(NODE) --test src/matcher.test.mjs src/native.test.mjs
	@echo "== differential: matcher.js vs the native book, seeds 7 and 11 at 200k ops"
	$(NODE) diff.mjs
	@echo "== C++ commit tree tests (keccak vectors, the Merkle-treap, the C ABI)"
	./commit_test
	@echo "== Node commit tree tests (the JavaScript reference)"
	cd ../../ && $(NODE) --test src/miner/commit.test.mjs
	@echo "== differential: commit.js vs the native commit tree, seeds 3 and 5 at 100k ops"
	$(NODE) commit_diff.mjs

bench-run: bench bench3
	@echo "== C++ -O2"; ./bench mixed 1000000
	@echo "== C++ -O3 -march=native"; ./bench3 mixed 1000000
	@echo "== Node, the identical workload"; $(NODE) bench.mjs mixed 1000000

commit-bench: commit_bench commit_bench3
	@echo "== C++ -O2"; ./commit_bench 1000000
	@echo "== C++ -O3 -march=native"; ./commit_bench3 1000000
	@echo "== Node, 100k inserts / 50k fills / 50k removes of the same workload"; $(NODE) commit_bench.mjs 100000

bench10: bench3
	./bench3 mixed 10000000
	./bench3 patho 100000

asan: test.cpp trace.cpp commit_test.cpp $(SRC) $(HDR) $(CSRC) $(CHDR)
	$(CXX) $(STD) -O1 -g -fsanitize=address,undefined $(WARN) test.cpp $(SRC) -o book_test_asan
	$(CXX) $(STD) -O1 -g -fsanitize=address,undefined $(WARN) trace.cpp $(SRC) -o trace_asan
	$(CXX) $(STD) -O1 -g -fsanitize=address,undefined $(WARN) commit_test.cpp $(CSRC) -o commit_test_asan
	./book_test_asan
	./trace_asan 7 50000 --summary | tail -1
	./commit_test_asan

clean:
	rm -f $(BINS) book_test_asan trace_asan commit_test_asan *.o
	rm -rf .journal-test

.PHONY: all test bench-run commit-bench bench10 asan clean
