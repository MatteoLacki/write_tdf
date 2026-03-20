# write_tdf — `pmsms2tdf`

C++23 command-line tool that converts two mmappet datasets (MS1 + MS2) into a
complete Bruker `.d` folder containing `analysis.tdf_bin` (ZSTD-compressed frame
binary) and `analysis.tdf` (SQLite metadata), ready for downstream tools like
OpenTIMS or Bruker DataAnalysis.

> **Note for AI assistants:** This file is the authoritative reference for the
> `write_tdf` sub-project. A higher-level summary of this tool also exists in
> `../../CLAUDE.md` (two directories up, at the `massimo_pipeline` repo root).
> When making changes here that affect the build system, dependencies, CLI
> interface, or key file list, consider whether the corresponding section in
> `../../CLAUDE.md` needs updating too — but only propagate changes if the user
> explicitly approves.

---

## Repository layout

```
write_tdf/
├── main.cpp                        # CLI, frame index, multi-threaded orchestration
├── src/
│   ├── tdf_writer.h                # TdfWriter: ZSTD frame encoder
│   ├── write_analysis_tdf.h        # write_analysis_tdf(): SQLite metadata writer
│   ├── mmappet.h                   # header-only memory-mapped column reader/writer
│   ├── zstd_bundled/               # vendored zstd source (gitignored, downloaded on first build)
│   └── sqlite_amalgamation/        # SQLite amalgamation (fallback if no system SQLite)
├── tests/
│   ├── roundtrip_test.py           # OpenTIMS round-trip verification
│   ├── F9477.d/                    # reference Bruker .d folder
│   ├── F9477_ms1.mmappet           # pre-extracted MS1 events (frame/scan/tof/intensity)
│   └── F9477_ms2.mmappet           # pre-extracted MS2 events
├── Makefile
├── LICENSE                         # MIT (project) + BSD-2 (zstd) + Public Domain (SQLite)
└── .gitignore
```

---

## Build system (`Makefile`)

### Dependencies

| Dependency | How resolved |
|------------|-------------|
| **mmappet.h** | Three-tier: `MMAPPET_H=/path/to/mmappet.h` env var → sibling repo `../mmappet/src/mmappet/cpp/mmappet/mmappet.h` (auto-detected via `$(wildcard ...)`) → bundled `src/mmappet.h`. |
| **zstd** | System by default: `ZSTD_PREFIX=/path` → `pkg-config libzstd` → bare `-lzstd`. Requires `libzstd-dev` (or equivalent) to be installed. |
| **SQLite** | Three-tier: `SQLITE3_PREFIX` env var → `pkg-config sqlite3` → amalgamation (`src/sqlite_amalgamation/`). |
| **pthreads** | System library, always linked via `-lpthread`. |

### Key variables

```makefile
ZSTD_VERSION := 1.5.6
ZSTD_SRC     := src/zstd_bundled/zstd-$(ZSTD_VERSION)
```

### Targets

```bash
make              # build pmsms2tdf (downloads+compiles zstd on first run)
make test         # roundtrip test against tests/F9477.d
make clean        # remove pmsms2tdf, *.o, src/zstd_bundled/  (add to Makefile if needed)

ZSTD_PREFIX=/usr make   # link against system zstd instead of vendored
```

---

## Data formats

### Input: mmappet (columnar memory-mapped)

An mmappet directory contains:
- `schema.txt` — one `<type> <name>` line per column (e.g. `uint32 frame`)
- `0.bin`, `1.bin`, … — raw binary column files (one element per row, little-endian)

`OpenColumn<T>(path, "column_name")` in `mmappet.h` reads `schema.txt` to find the
column index, then memory-maps the corresponding `.bin` file.

**Required columns** (both MS1 and MS2 inputs):

| Column | Type | Description |
|--------|------|-------------|
| `frame` | `uint32` | Bruker frame ID (must be pre-sorted ascending) |
| `scan` | `uint32` | Scan index within frame (0-based, ion mobility dimension) |
| `tof` | `uint32` | TOF bin index (m/z dimension, instrument-native) |
| `intensity` | `uint32` | Peak intensity |

### Output: Bruker `.d` folder

```
output.d/
├── analysis.tdf_bin          # frame binary (Bruker TIMS format, ZSTD-compressed)
├── analysis.tdf              # SQLite metadata
└── frames_metadata.mmappet/  # per-frame stats (Id, TimsId, NumPeaks, MaxIntensity, SummedIntensities)
```

---

## Binary format (`analysis.tdf_bin`)

Each frame is one contiguous block:

```
[block_size: uint32]          # total bytes of this block = comp_size + 8
[total_scans: uint32]         # instrument scan count (same for all frames)
[compressed_payload: bytes]   # ZSTD-compressed transposed data
```

**Uncompressed payload layout** (`back_data`, before transposition):

```
[peak_cnts: uint32 × total_scans]     # scan→peak-count header (see below)
[tof_delta_0: uint32]                 # TOF delta for event 0
[intensity_0: uint32]                 # intensity for event 0
[tof_delta_1: uint32]
[intensity_1: uint32]
...
```

**peak_cnts construction:**
- `peak_cnts[0] = total_scans`  (Bruker convention: slot 0 holds the total scan count)
- `peak_cnts[s+1] += 2` for each event in scan `s`  (×2 because interleaved tof+intensity)
- i.e. `peak_cnts[s]` (for `s≥1`) = 2 × (number of events in scan `s-1`)

**TOF delta encoding:** within each scan, events are stored as `tof - prev_tof`
(wrapping from `uint32(-1)` at the start of each new scan).

**4-byte lane transpose** (`back_data` → `real_data`): bytes are re-ordered so that
byte 0 of every uint32 comes first, then byte 1, etc. This significantly improves
ZSTD compression ratio on instrument data.

```
back_data:  [B0 B1 B2 B3] [B0 B1 B2 B3] ...   (N uint32s = 4N bytes)
real_data:  [B0 B0 B0 ...] [B1 B1 B1 ...] [B2 ...] [B3 ...]
```

Implementation (from `tdf_writer.h`):
```cpp
size_t reminder = 0, bd_idx = 0;
for (size_t rd_idx = 0; rd_idx < back_size; ++rd_idx) {
    if (bd_idx >= back_size) { ++reminder; bd_idx = reminder; }
    real_data_[rd_idx] = back_data_[bd_idx];
    bd_idx += 4;
}
```

---

## Key source files

### `main.cpp`

Roles:
1. **CLI parsing** — `parse_args()` fills `Config`; see `usage()` for all flags.
2. **Frame index building** — `build_frame_index()` zip-merges MS1+MS2 `FrameEntry`
   lists (sorted by `frame_id`), fills gaps with `EMPTY` entries, caps at
   `max_frames`. Aborts on duplicate or unsorted frame IDs.
3. **Dry-run mode** — omit `--output` to print tab-separated events to stdout
   instead of writing files.
4. **Multi-threaded write** — `write_tdf()` partitions the frame index into
   `N_threads` slices; each thread runs a `TdfWriter` on its own temp binary file.
   After all threads join, per-thread `TimsId` byte offsets are fixed up in memory
   and the temp files are concatenated into the final `analysis.tdf_bin`.
5. **SQLite write** — calls `write_analysis_tdf()` to copy the template
   `analysis.tdf` and repopulate dynamic tables.
6. **Metadata mmappet** — writes `frames_metadata.mmappet` unless `--drop-metadata`.

**`FrameEntry`** (defined in `main.cpp`):
```cpp
enum Source : int8_t { MS1 = 0, MS2 = 1, EMPTY = -1 };
struct FrameEntry { uint32_t frame_id; Source source; size_t start; size_t end; };
// start/end: row range [start, end) into the ms1 or ms2 column arrays
```

### `src/tdf_writer.h` — `TdfWriter`

Stateful per-thread frame writer. Owns one `FILE*` (the per-thread `.tdf_bin`
fragment). Caller passes pre-allocated metadata spans; `write_frame()` fills
slot `frames_written_` on each call.

Reusable scratch buffers (`peak_cnts_`, `tof_deltas_`, `interleaved_`,
`back_data_`, `real_data_`, `compress_buf_`) grow as needed, never shrink —
avoids repeated allocations across frames.

### `src/write_analysis_tdf.h` — `write_analysis_tdf()`

Header-only, included after `FrameEntry`/`Source` are defined in `main.cpp`.

Steps (in order):
1. `fs::copy_file` template `analysis.tdf` → `output_dir/analysis.tdf`
2. `sqlite3_open` the copy
3. Read T1/T2/Pressure defaults from template `Frames` (overridable via
   `AnalysisTdfConfig`)
4. Read representative `FrameProperties` for MS1 (MsMsType=0) and MS2
   (MsMsType=9) frames from the template
5. `DELETE FROM Frames; DELETE FROM DiaFrameMsMsInfo; DELETE FROM Segments;
   DELETE FROM FrameProperties;` — all in one transaction
6. `INSERT INTO Frames` — one row per `FrameEntry`; frame time computed as
   `(frame_id - first_fid) × (accum_ms + ramp_ms) / 1000`; MsMsType=9 for MS2,
   0 otherwise
7. `INSERT INTO FrameProperties` — replicate representative MS1/MS2 property
   set for each frame
8. `INSERT INTO DiaFrameMsMsInfo` — cycling `WindowGroup` 1..N_wg for MS2 frames
9. `INSERT INTO Segments` — single segment spanning first..last frame_id
10. Optionally `UPDATE TimsCalibration` C0..C9 (`--calib-params`)
11. Optionally replace `DiaFrameMsMsWindows` / `DiaFrameMsMsWindowGroups` from
    an mmappet (`--dia-windows`)
12. `COMMIT`

### `src/mmappet.h`

Header-only C++23 library. Key entry points used by `main.cpp`:

```cpp
// Open a single column by name (reads schema.txt, mmaps <col_nr>.bin):
MMappedData<uint32_t> col = OpenColumn<uint32_t>(path, "frame");

// Write a new mmappet from parallel arrays:
auto writer = Schema<uint32_t, uint64_t, uint32_t, uint32_t, uint64_t>(
    "Id", "TimsId", "NumPeaks", "MaxIntensity", "SummedIntensities"
).create_writer(output_dir / "frames_metadata.mmappet");
writer.write_rows(n, ids, tims_ids, num_peaks, max_ints, sum_ints);
```

---

## CLI reference

```
./pmsms2tdf [--ms1 <ms1.mmappet>] [--ms2 <ms2.mmappet>]
            (at least one of --ms1 / --ms2 required)
            [--output <output.d>] [--tdf <template.d/analysis.tdf>]
            [options]
```

`--ms1` and `--ms2` are each optional; at least one must be provided.
`--output` triggers write mode; omitting it prints events to stdout (dry-run).
`--tdf` is required when `--output` is given.

| Flag | Default | Description |
|------|---------|-------------|
| `--tdf PATH` | (required with --output) | Template `analysis.tdf` |
| `--write-metadata` | false | Write `frames_metadata.mmappet` |
| `--max-frames N` | all | Cap on merged frame count |
| `--threads N` | all cores | Parallel writer threads |
| `--zstd-level N` | 1 | ZSTD compression level (1–22) |
| `--verbose` | false | Print frame index head/tail + per-frame stats |
| `--use-fread` | false | Use `fread`/`fwrite` instead of `copy_file_range` for concatenation |
| `--T1/--T2/--Pressure F` | from template | Override per-frame instrument params |
| `--accumulation-time F` | 100.0 | AccumulationTime (ms) |
| `--ramp-time F` | 100.0 | RampTime (ms) |
| `--polarity C` | `+` | `+` or `-` |
| `--scan-mode N` | 9 | ScanMode for all frames |
| `--calib-params C0,..,C9` | (none) | 10 comma-separated floats |
| `--dia-windows PATH` | (none) | mmappet replacing DiaFrameMsMsWindows |

---

## Test

```bash
make test
# Runs:
#   ./pmsms2tdf --ms1 tests/F9477_ms1.mmappet --ms2 tests/F9477_ms2.mmappet \
#               --output /tmp/pmsms2tdf_roundtrip.d \
#               --tdf tests/F9477.d/analysis.tdf
#   python tests/roundtrip_test.py tests/F9477.d /tmp/pmsms2tdf_roundtrip.d
# Expected output: "All 9560 frames match."
```

`roundtrip_test.py` uses `opentimspy` to read both the original and reconstructed
`.d` folders and compares `scan`, `tof`, and `intensity` arrays frame by frame.

---

## Design notes

- **No external zstd required.** The Makefile vendors zstd 1.5.6 by downloading
  the official tarball and building `libzstd.a` statically. The `src/zstd_bundled/`
  directory is gitignored. Use `ZSTD_PREFIX=/path make` to override.
- **Thread safety.** Each thread writes to its own temporary file; the main thread
  concatenates after joining. `TimsId` (byte offset in the final binary) is fixed
  up in memory after file sizes are known.
- **Gap frames.** If frame IDs are not contiguous across MS1/MS2, `build_frame_index`
  inserts `EMPTY` entries. `TdfWriter::write_frame` accepts empty spans and writes a
  zero-event frame block (valid Bruker format).
- **`write_analysis_tdf.h` dependency on `main.cpp` types.** `FrameEntry`, `Source`,
  `MS1`, `MS2`, `EMPTY` must be defined before `#include "write_analysis_tdf.h"`.
  This is intentional (avoids a separate header for these small POD types).
- **`copy_file_range` on Linux.** Used by default for efficient kernel-space
  file concatenation. Falls back to `fread`/`fwrite` via `--use-fread` or on
  non-Linux platforms (the `#ifdef __linux__` guard).
