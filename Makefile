CXX      := clang++
CXXFLAGS := -std=c++23 -O3 -Wall -Isrc
LDFLAGS  := -lzstd
PYTHON   := ../../venvs/common/bin/python
TEST_IN  := ../../temp/test/deduplicated_precursors.mmappet
TEST_OUT := /tmp/pmsms2tdf_test.d

pmsms2tdf: main.o
	$(CXX) $^ -o $@ $(LDFLAGS)

main.o: main.cpp
	$(CXX) $(CXXFLAGS) -c $<

test: pmsms2tdf
	./pmsms2tdf $(TEST_IN) $(TEST_OUT)/analysis.tdf_bin
	$(PYTHON) tests/check_output.py $(TEST_IN) $(TEST_OUT)

.PHONY: test
