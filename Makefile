# -----------------------------------------------------------------------------
# CMake project wrapper Makefile ----------------------------------------------
# -----------------------------------------------------------------------------

RM    := rm -rf
MKDIR := mkdir -p

all: deps rel

rel: ./build/Makefile
	@ $(MAKE) -C build

debug: ./build/Makefile-debug
	@ $(MAKE) -C build-debug

./build/Makefile:
	@  ($(MKDIR) build > /dev/null)
	@  (cd build > /dev/null 2>&1 && cmake -DCMAKE_BUILD_TYPE=Release ../src)

./build/Makefile-debug:
	@  ($(MKDIR) build-debug > /dev/null)
	@  (cd build-debug > /dev/null 2>&1 && cmake -DCMAKE_BUILD_TYPE=Debug -DDEBUG=true ../src)

clean:
	@- $(RM) -rf ./build
	@- $(RM) -rf ./build-debug

.PHONY: test
.PHONY: deps

deps:
	@ $(MAKE) -C deps

marlin: rel
	@./build/main/marlin

run: rel
	@./build/main/marlin

rund: debug
	@./build-debug/main/marlin

valgrind: debug
	@valgrind -v --tool=memcheck --leak-check=full --valgrind-stacksize=10485760 --show-possibly-lost=no ./build-debug/main/marlin
	#@valgrind -v --tool=memcheck --valgrind-stacksize=10485760 --show-possibly-lost=no ./build-debug/main/marlin

callgrind: debug
	@valgrind --tool=callgrind -v ./build-debug/main/marlin

helgrind: debug
	@valgrind --tool=helgrind -v ./build-debug/main/marlin

testsetup: rel
	@- $(RM) -rf ./db/acme/test/s_*
	@  echo "Generating test data ..."
	@  (cd test && python data_gen.py > test.json)
	@  echo "Generated test data ..."

test: testsetup
	@  (cd test && python test.py)

coverage: testsetup debug
	@  (cd build-debug && cp ../test/test.json . && cp -R ../test/robot . && make marlin_coverage)

testlive:
	@  (cd test && python test.py live)

profile: debug
	@  (./test/profile.sh)
	@  echo "Use killall -12 marlin to start and stop profiling"

profileread:
	@  pprof --web ./build-debug/src/main/marlin test.prof.0

bench: testsetup
	@  (cd test && python test.py bench)

