# pmsms2tdf

Convert simulated (or processed) MS1/MS2 event data into a complete **Bruker `.d`
folder** — both the ZSTD-compressed frame binary (`analysis.tdf_bin`) and the
SQLite metadata file (`analysis.tdf`) — ready to open in OpenTIMS, Bruker
DataAnalysis, or any other tool that speaks the timsTOF data format.

---

## What it does

timsTOF raw data is stored as a directory of files (`.d`). The binary core is
`analysis.tdf_bin`: a sequence of ZSTD-compressed frames, each encoding ion
mobility scans × TOF peaks. The metadata lives in `analysis.tdf` (SQLite).

`pmsms2tdf` takes two columnar memory-mapped datasets (one for MS1, one for MS2),
merges their frame sequences, fills any gaps, and writes a fully valid `.d` folder
that round-trips perfectly through OpenTIMS.

```
ms1.mmappet ──┐
               ├─▶  pmsms2tdf  ──▶  output.d/
ms2.mmappet ──┘                       ├── analysis.tdf_bin   (frame binary)
                                       ├── analysis.tdf       (SQLite metadata)
                                       └── frames_metadata.mmappet
```

---

## Build

**Requirements:** `clang++` (C++23), `clang` (C compiler), `curl`, `tar`, `make`.
No other system dependencies are required — zstd is vendored automatically.

```bash
git clone https://github.com/yourorg/write_tdf
cd write_tdf
make
```

On the first build, `make` downloads the zstd 1.5.6 source tarball, builds a
static `libzstd.a`, then compiles `pmsms2tdf`. Subsequent builds skip the
download and use the cached library.

```bash
make test   # round-trip verification against the bundled test dataset
```

Expected output:

```
frames built: 9560
wrote 9560 frames to /tmp/pmsms2tdf_roundtrip.d/analysis.tdf_bin
wrote analysis.tdf (9560 frames)

All 9560 frames match.
```

---

## CLI usage

```
./pmsms2tdf --ms1 <ms1.mmappet> --ms2 <ms2.mmappet>
            --output <output.d> --tdf <template.d/analysis.tdf>
            [options]
```

`--output` and `--tdf` together trigger write mode. Omit both for a **dry run**
that prints the merged event table to stdout instead of writing any files.

### Required

| Flag | Description |
|------|-------------|
| `--ms1 PATH` | MS1 mmappet directory (`frame`, `scan`, `tof`, `intensity` columns) |
| `--ms2 PATH` | MS2 mmappet directory (same schema) |
| `--output PATH` | Output `.d` directory to create |
| `--tdf PATH` | Template `analysis.tdf` to copy instrument metadata from |

### Options

| Flag | Default | Description |
|------|---------|-------------|
| `--drop-metadata` | — | Skip writing `frames_metadata.mmappet` |
| `--threads N` | all cores | Parallel writer threads |
| `--zstd-level N` | 1 | ZSTD compression level (1–22) |
| `--max-frames N` | all | Cap the merged frame count |
| `--accumulation-time F` | 100.0 | AccumulationTime in ms |
| `--ramp-time F` | 100.0 | RampTime in ms |
| `--T1 / --T2 / --Pressure F` | from template | Override instrument parameters |
| `--polarity C` | `+` | Ion polarity (`+` or `-`) |
| `--scan-mode N` | 9 | ScanMode written to all frames |
| `--calib-params C0,..,C9` | from template | Override TimsCalibration coefficients |
| `--dia-windows PATH` | from template | mmappet replacing DiaFrameMsMsWindows |
| `--verbose` | — | Print frame index and per-frame statistics |
| `--use-fread` | — | Use `fread`/`fwrite` instead of `copy_file_range` |

### Examples

```bash
# Basic conversion
./pmsms2tdf \
  --ms1 data/run1_ms1.mmappet \
  --ms2 data/run1_ms2.mmappet \
  --output results/run1.d \
  --tdf reference.d/analysis.tdf

# Override calibration and compress harder
./pmsms2tdf \
  --ms1 data/run1_ms1.mmappet \
  --ms2 data/run1_ms2.mmappet \
  --output results/run1.d \
  --tdf reference.d/analysis.tdf \
  --calib-params 0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9,1.0 \
  --zstd-level 9

# Dry run: inspect merged event order without writing files
./pmsms2tdf \
  --ms1 data/run1_ms1.mmappet \
  --ms2 data/run1_ms2.mmappet \
  | head -20
```

---

## Input format: mmappet

An mmappet directory is a flat set of binary column files plus a schema:

```
run1_ms1.mmappet/
├── schema.txt   # one "type name" line per column, e.g. "uint32 frame"
├── 0.bin        # frame column  (uint32, row-major)
├── 1.bin        # scan column   (uint32)
├── 2.bin        # tof column    (uint32)
└── 3.bin        # intensity column (uint32)
```

Both inputs must be **sorted by `frame` ascending**. Within each frame, events
can be in any order.

---

## Using the C++ headers

The two core headers — `src/tdf_writer.h` and `src/mmappet.h` — can be used
independently in other C++23 projects.

### Read mmappet columns

```cpp
#include "mmappet.h"

// Open a single typed column by name
auto frames = OpenColumn<uint32_t>("run1_ms1.mmappet", "frame");
auto tofs   = OpenColumn<uint32_t>("run1_ms1.mmappet", "tof");

for (size_t i = 0; i < frames.size(); ++i)
    std::print("{}\t{}\n", frames[i], tofs[i]);
```

### Write an mmappet dataset

```cpp
#include "mmappet.h"

auto writer = Schema<uint32_t, uint32_t, uint32_t, uint32_t>(
    "frame", "scan", "tof", "intensity"
).create_writer("output.mmappet");

writer.write_rows(n_events, frame_data, scan_data, tof_data, intensity_data);
```

### Encode frames with `TdfWriter`

```cpp
#include "tdf_writer.h"

// Pre-allocate per-frame metadata arrays
std::vector<uint32_t> ids(n_frames), num_peaks(n_frames), max_ints(n_frames);
std::vector<uint64_t> tims_ids(n_frames), sum_ints(n_frames);

TdfWriter writer(
    "output.d/analysis.tdf_bin",
    total_scans,
    {ids.data(),       n_frames},
    {tims_ids.data(),  n_frames},
    {num_peaks.data(), n_frames},
    {max_ints.data(),  n_frames},
    {sum_ints.data(),  n_frames},
    /*verbose=*/false, /*zstd_level=*/1);

for (size_t i = 0; i < n_frames; ++i)
    writer.write_frame(frame_ids[i], scans[i], tofs[i], intensities[i]);
// tims_ids now holds byte offsets into analysis.tdf_bin
```

---

## Dependency management

All dependencies are resolved at build time with no manual installation required.

### mmappet.h (three-tier)

| Priority | Condition | Source |
|----------|-----------|--------|
| 1 | `MMAPPET_H=/path/to/mmappet.h` set | User-supplied header |
| 2 | `../mmappet/src/mmappet/cpp/mmappet/mmappet.h` exists | Sibling repo (auto-detected) |
| 3 | *(fallback)* | Bundled `src/mmappet.h` |

### zstd (two-tier)

| Priority | Condition | Source |
|----------|-----------|--------|
| 1 | `ZSTD_PREFIX=/path` set | System or custom installation |
| 2 | *(fallback)* | Vendored: downloaded and built as `libzstd.a` on first `make` |

### SQLite (three-tier)

| Priority | Condition | Source |
|----------|-----------|--------|
| 1 | `SQLITE3_PREFIX=/path` set | User-supplied installation |
| 2 | `pkg-config sqlite3` succeeds | System SQLite |
| 3 | *(fallback)* | Bundled amalgamation (`src/sqlite_amalgamation/`) |

### pthreads

Always linked from the system (`-lpthread`). No extra setup needed.

---

## License

MIT — see [LICENSE](LICENSE).

Third-party components bundled or statically linked:
- **zstd** — BSD 2-Clause © Meta Platforms, Inc.
- **SQLite** — Public Domain
