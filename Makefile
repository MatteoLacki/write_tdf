CXX      := clang++
CC       := clang
CXXFLAGS := -std=c++23 -O3 -Wall -Isrc
LDFLAGS  := -lzstd -lpthread
PYTHON   := ../../venvs/common/bin/python
MS1_IN   := tests/F9477_ms1.mmappet
MS2_IN   := tests/F9477_ms2.mmappet
TEST_OUT := /tmp/pmsms2tdf_roundtrip.d
TDF_SRC  := tests/F9477.d

# ---------------------------------------------------------------------------
# SQLite detection (three-tier: user prefix → pkg-config → bundled amalgamation)
# ---------------------------------------------------------------------------
ifdef SQLITE3_PREFIX
  SQLITE3_CFLAGS := -I$(SQLITE3_PREFIX)/include
  SQLITE3_LIBS   := -L$(SQLITE3_PREFIX)/lib -lsqlite3
else
  SQLITE3_CFLAGS := $(shell pkg-config --cflags sqlite3 2>/dev/null)
  SQLITE3_LIBS   := $(shell pkg-config --libs   sqlite3 2>/dev/null)
endif
ifeq ($(SQLITE3_LIBS),)
  SQLITE3_CFLAGS    := -Isrc/sqlite_amalgamation
  SQLITE3_LIBS      := src/sqlite_amalgamation/sqlite3.o
  AMALGAMATION_DEPS := src/sqlite_amalgamation/sqlite3.o
endif

CXXFLAGS += $(SQLITE3_CFLAGS)
LDFLAGS  += $(SQLITE3_LIBS)

# ---------------------------------------------------------------------------
# Targets
# ---------------------------------------------------------------------------

pmsms2tdf: main.o $(AMALGAMATION_DEPS)
	$(CXX) $^ -o $@ $(LDFLAGS)

main.o: main.cpp
	$(CXX) $(CXXFLAGS) -c $<

src/sqlite_amalgamation/sqlite3.o: src/sqlite_amalgamation/sqlite3.c
	$(CC) -O2 -c $< -o $@

test: pmsms2tdf
	rm -rf $(TEST_OUT)
	./pmsms2tdf --ms1 $(MS1_IN) --ms2 $(MS2_IN) --output $(TEST_OUT) --tdf $(TDF_SRC)/analysis.tdf
	$(PYTHON) tests/roundtrip_test.py $(TDF_SRC) $(TEST_OUT)

.PHONY: test
