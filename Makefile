CXX      := clang++
CXXFLAGS := -std=c++23 -O3 -Wall -Isrc
LDFLAGS  := -lzstd -lpthread
PYTHON   := ../../venvs/common/bin/python
MS1_IN   := tests/F9477_ms1.mmappet
MS2_IN   := tests/F9477_ms2.mmappet
TEST_OUT := /tmp/pmsms2tdf_roundtrip.d
TDF_SRC  := tests/F9477.d

pmsms2tdf: main.o
	$(CXX) $^ -o $@ $(LDFLAGS)

main.o: main.cpp
	$(CXX) $(CXXFLAGS) -c $<

test: pmsms2tdf
	./pmsms2tdf $(MS1_IN) $(MS2_IN) $(TEST_OUT)
	cp $(TDF_SRC)/analysis.tdf $(TEST_OUT)/analysis.tdf
	$(PYTHON) tests/update_frames_table.py $(TEST_OUT)
	$(PYTHON) tests/roundtrip_test.py $(TDF_SRC) $(TEST_OUT)

.PHONY: test
