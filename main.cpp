#include "tdf_writer.h"
#include "mmappet.h"
#include <iostream>
#include <format>
#include <filesystem>
#include <vector>
#include <optional>
#include <algorithm>
#include <cstdlib>
#include <string_view>
#include <thread>
#include <atomic>
#ifdef __linux__
#  include <sys/stat.h>
#  include <fcntl.h>
#endif


// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

enum Source : int8_t { MS1 = 0, MS2 = 1, EMPTY = -1 };
struct FrameEntry { uint32_t frame_id; Source source; size_t start; size_t end; };

// write_analysis_tdf.h uses FrameEntry/Source/MS1/MS2/EMPTY defined above.
#include "write_analysis_tdf.h"

struct Config {
    std::optional<std::filesystem::path> ms1_path;
    std::optional<std::filesystem::path> ms2_path;
    std::optional<std::filesystem::path> output_dir;  // absent = dry-run
    std::optional<std::filesystem::path> tdf_src;     // template analysis.tdf
    bool write_metadata = false;
    size_t max_frames = SIZE_MAX;
    size_t threads = std::thread::hardware_concurrency();
    int zstd_level = 1;
    bool verbose = false;
    bool use_fread = false;
    AnalysisTdfConfig atdf_cfg;
};


// ---------------------------------------------------------------------------
// CLI
// ---------------------------------------------------------------------------

[[noreturn]] static void usage(const char* prog, int code) {
    std::cerr << std::format(
        "pmsms2tdf -- convert MS1/MS2 mmappet event data into a Bruker timsTOF .d folder.\n"
        "\n"
        "Reads one or two columnar memory-mapped datasets (MS1 and/or MS2), merges their\n"
        "frame sequences, fills any gaps with empty frames, encodes each frame as a\n"
        "ZSTD-compressed binary block (analysis.tdf_bin), and writes the accompanying\n"
        "SQLite metadata (analysis.tdf) by repopulating a user-supplied template.\n"
        "The output .d folder can be opened directly by OpenTIMS, Bruker DataAnalysis,\n"
        "or any other tool that reads the timsTOF data format.\n"
        "\n"
        "Usage: {} [--ms1 <ms1.mmappet>] [--ms2 <ms2.mmappet>]\n"
        "         (at least one of --ms1 / --ms2 required)\n"
        "         [--output <output.d/>] [--tdf <analysis.tdf>] [options]\n"
        "\n"
        "  --ms1 PATH           input MS1 mmappet (frame/scan/tof/intensity); optional\n"
        "  --ms2 PATH           input MS2 mmappet (frame/scan/tof/intensity); optional\n"
        "  --output PATH        output .d directory; omit to print events instead of writing\n"
        "  --tdf PATH           source analysis.tdf template to copy (required with --output)\n"
        "  --write-metadata     write frames_metadata.mmappet\n"
        "  --max-frames N       cap on merged+gap-filled frame sequence (default: all)\n"
        "  --threads N          number of parallel writer threads (default: all cores)\n"
        "  --zstd-level N       ZSTD compression level 1-22 (default: 1)\n"
        "  --verbose            print first/last 10 entries of the frame index\n"
        "  --use-fread          use fread/fwrite instead of copy_file_range\n"
        "  --T1 F               override T1 temperature for all frames\n"
        "  --T2 F               override T2 temperature for all frames\n"
        "  --Pressure F         override pressure for all frames\n"
        "  --accumulation-time F  AccumulationTime in ms (default: 100.0)\n"
        "  --ramp-time F        RampTime in ms (default: 100.0)\n"
        "  --denoised N         Denoised flag (default: 0)\n"
        "  --mz-calibration N   MzCalibration FK (default: 1)\n"
        "  --polarity C         Polarity '+' or '-' (default: '+')\n"
        "  --scan-mode N        ScanMode for all frames (default: 9)\n"
        "  --calib-params C0,..,C9  override TimsCalibration C0-C9 (10 comma-separated floats)\n"
        "  --dia-windows PATH   mmappet to replace DiaFrameMsMsWindows\n",
        prog);
    std::exit(code);
}

static Config parse_args(int argc, char** argv) {
    Config cfg;
    bool have_ms1 = false, have_ms2 = false;

    for (int i = 1; i < argc; ++i) {
        std::string_view arg(argv[i]);

        auto need_value = [&](std::string_view name) -> const char* {
            if (i + 1 >= argc) {
                std::cerr << "Error: " << name << " requires a value\n";
                std::exit(1);
            }
            return argv[++i];
        };

        if (arg == "--ms1") {
            cfg.ms1_path = need_value("--ms1");
            have_ms1 = true;
        } else if (arg == "--ms2") {
            cfg.ms2_path = need_value("--ms2");
            have_ms2 = true;
        } else if (arg == "--output") {
            cfg.output_dir = need_value("--output");
        } else if (arg == "--tdf") {
            cfg.tdf_src = need_value("--tdf");
        } else if (arg == "--write-metadata") {
            cfg.write_metadata = true;
        } else if (arg == "--max-frames") {
            char* end;
            unsigned long long v = strtoull(need_value("--max-frames"), &end, 10);
            if (*end != '\0' || v == 0) {
                std::cerr << "Error: --max-frames must be a positive integer\n";
                std::exit(1);
            }
            cfg.max_frames = (size_t)v;
        } else if (arg == "--threads") {
            char* end;
            unsigned long long v = strtoull(need_value("--threads"), &end, 10);
            if (*end != '\0' || v == 0) {
                std::cerr << "Error: --threads must be a positive integer\n";
                std::exit(1);
            }
            cfg.threads = (size_t)v;
        } else if (arg == "--zstd-level") {
            char* end;
            long v = strtol(need_value("--zstd-level"), &end, 10);
            if (*end != '\0' || v < 1 || v > 22) {
                std::cerr << "Error: --zstd-level must be an integer between 1 and 22\n";
                std::exit(1);
            }
            cfg.zstd_level = (int)v;
        } else if (arg == "--verbose") {
            cfg.verbose = true;
        } else if (arg == "--use-fread") {
            cfg.use_fread = true;
        } else if (arg == "--T1") {
            char* end;
            cfg.atdf_cfg.T1 = strtod(need_value("--T1"), &end);
        } else if (arg == "--T2") {
            char* end;
            cfg.atdf_cfg.T2 = strtod(need_value("--T2"), &end);
        } else if (arg == "--Pressure") {
            char* end;
            cfg.atdf_cfg.pressure = strtod(need_value("--Pressure"), &end);
        } else if (arg == "--accumulation-time") {
            char* end;
            cfg.atdf_cfg.accumulation_time_ms = strtod(need_value("--accumulation-time"), &end);
        } else if (arg == "--ramp-time") {
            char* end;
            cfg.atdf_cfg.ramp_time_ms = strtod(need_value("--ramp-time"), &end);

        } else if (arg == "--denoised") {
            char* end;
            cfg.atdf_cfg.denoised = (int)strtol(need_value("--denoised"), &end, 10);
        } else if (arg == "--mz-calibration") {
            char* end;
            cfg.atdf_cfg.mz_calibration = (int)strtol(need_value("--mz-calibration"), &end, 10);
        } else if (arg == "--polarity") {
            const char* v = need_value("--polarity");
            if (v[0] != '+' && v[0] != '-') {
                std::cerr << "Error: --polarity must be '+' or '-'\n";
                std::exit(1);
            }
            cfg.atdf_cfg.polarity = v[0];
        } else if (arg == "--scan-mode") {
            char* end;
            cfg.atdf_cfg.scan_mode = (int)strtol(need_value("--scan-mode"), &end, 10);
        } else if (arg == "--calib-params") {
            const char* v = need_value("--calib-params");
            std::array<double, 10> c{};
            char* p = const_cast<char*>(v);
            for (int k = 0; k < 10; ++k) {
                char* endp;
                c[k] = strtod(p, &endp);
                if (endp == p) {
                    std::cerr << "Error: --calib-params requires 10 comma-separated floats\n";
                    std::exit(1);
                }
                p = endp;
                if (k < 9) {
                    if (*p != ',') {
                        std::cerr << "Error: --calib-params: expected ',' after value " << k << "\n";
                        std::exit(1);
                    }
                    ++p;
                }
            }
            cfg.atdf_cfg.calib_params = c;
        } else if (arg == "--dia-windows") {
            cfg.atdf_cfg.dia_windows_path = need_value("--dia-windows");
        } else if (arg == "--help" || arg == "-h") {
            usage(argv[0], 0);
        } else {
            std::cerr << "Error: unknown option " << arg << "\n";
            usage(argv[0], 1);
        }
    }

    if (!have_ms1 && !have_ms2) {
        std::cerr << "Error: at least one of --ms1 or --ms2 is required\n";
        usage(argv[0], 1);
    }
    if (cfg.output_dir && !cfg.tdf_src) {
        std::cerr << "Error: --tdf is required when --output is given\n";
        usage(argv[0], 1);
    }
    return cfg;
}


// ---------------------------------------------------------------------------
// Frame index building
// ---------------------------------------------------------------------------

// Collect FrameEntry records from a pre-sorted frame column.
static void collect_frame_entries(
    std::span<const uint32_t> frames,
    Source src,
    std::vector<FrameEntry>& out)
{
    size_t i = 0;
    while (i < frames.size()) {
        uint32_t fid = frames[i];
        size_t j = i + 1;
        while (j < frames.size() && frames[j] == fid) ++j;
        out.push_back({fid, src, i, j});
        i = j;
    }
}

// Build the merged, gap-filled, and capped frame index.
static std::vector<FrameEntry> build_frame_index(
    std::span<const uint32_t> ms1_frames,
    std::span<const uint32_t> ms2_frames,
    size_t max_frames,
    Source gap_source = EMPTY)
{
    std::vector<FrameEntry> ms1_entries, ms2_entries;
    collect_frame_entries(ms1_frames, MS1, ms1_entries);
    collect_frame_entries(ms2_frames, MS2, ms2_entries);

    // Zip-merge with on-the-fly gap-fill, sortedness check, and cap.
    std::vector<FrameEntry> idx;
    idx.reserve(ms1_entries.size() + ms2_entries.size());
    size_t i = 0, j = 0;
    while ((i < ms1_entries.size() || j < ms2_entries.size()) && idx.size() < max_frames) {
        bool take_ms1 = (j == ms2_entries.size()) ||
                        (i < ms1_entries.size() && ms1_entries[i].frame_id < ms2_entries[j].frame_id);
        FrameEntry& fe = take_ms1 ? ms1_entries[i] : ms2_entries[j];

        if (!idx.empty() && fe.frame_id <= idx.back().frame_id) {
            if (fe.frame_id == idx.back().frame_id)
                std::cerr << "Error: frame_id " << fe.frame_id
                          << " appears in both ms1 and ms2 inputs\n";
            else
                std::cerr << "Error: " << (take_ms1 ? "ms1" : "ms2")
                          << " frames are not sorted (frame_id " << fe.frame_id
                          << " after " << idx.back().frame_id << ")\n";
            std::exit(1);
        }

        // Fill gap before this entry
        uint32_t prev_fid = idx.empty() ? fe.frame_id : idx.back().frame_id;
        for (uint32_t fid = prev_fid + 1; fid < fe.frame_id && idx.size() < max_frames; ++fid)
            idx.push_back({fid, gap_source, 0, 0});

        if (idx.size() < max_frames)
            idx.push_back(fe);
        take_ms1 ? ++i : ++j;
    }

    std::cout << std::format("frames built: {}\n", idx.size());
    return idx;
}


// ---------------------------------------------------------------------------
// Printing helpers
// ---------------------------------------------------------------------------

static void print_frame_index(const std::vector<FrameEntry>& frame_index) {
    size_t n = frame_index.size();
    size_t head = std::min(n, size_t(10));
    size_t tail_start = std::max(head, n > 10 ? n - 10 : n);
    std::cout << "frame_index:\n";
    for (size_t k = 0; k < head; ++k) {
        const FrameEntry& fe = frame_index[k];
        const char* src = (fe.source == MS1) ? "ms1" : (fe.source == MS2) ? "ms2" : "empty";
        std::cout << std::format("  [{}] frame={}  src={}  rows=[{},{})\n", k, fe.frame_id, src, fe.start, fe.end);
    }
    if (tail_start > head) std::cout << "  ...\n";
    for (size_t k = tail_start; k < n; ++k) {
        const FrameEntry& fe = frame_index[k];
        const char* src = (fe.source == MS1) ? "ms1" : (fe.source == MS2) ? "ms2" : "empty";
        std::cout << std::format("  [{}] frame={}  src={}  rows=[{},{})\n", k, fe.frame_id, src, fe.start, fe.end);
    }
}

static void print_dry_run(
    const std::vector<FrameEntry>& frame_index,
    std::span<const uint32_t> ms1_scans,
    std::span<const uint32_t> ms1_tofs,
    std::span<const uint32_t> ms1_ints,
    std::span<const uint32_t> ms2_scans,
    std::span<const uint32_t> ms2_tofs,
    std::span<const uint32_t> ms2_ints)
{
    std::cout << "frame\tscan\ttof\tintensity\n";
    for (const auto& fe : frame_index) {
        if (fe.source == EMPTY) continue;
        std::span<const uint32_t> scans = (fe.source == MS1) ? ms1_scans : ms2_scans;
        std::span<const uint32_t> tofs  = (fe.source == MS1) ? ms1_tofs  : ms2_tofs;
        std::span<const uint32_t> ints  = (fe.source == MS1) ? ms1_ints  : ms2_ints;
        for (size_t i = fe.start; i < fe.end; ++i)
            std::cout << std::format("{}\t{}\t{}\t{}\n", fe.frame_id, scans[i], tofs[i], ints[i]);
    }
}


// ---------------------------------------------------------------------------
// Merge helpers
// ---------------------------------------------------------------------------

// Append contents of file `src` to already-open FILE* `dst`.
static bool append_file(FILE* dst, const std::filesystem::path& src, bool use_fread = false) {
#ifdef __linux__
    if (!use_fread) {
        int src_fd = open(src.c_str(), O_RDONLY);
        if (src_fd < 0) {
            std::cerr << "append_file: cannot open " << src << "\n";
            return false;
        }
        struct stat st;
        if (fstat(src_fd, &st) < 0) {
            close(src_fd);
            std::cerr << "append_file: fstat failed: " << src << "\n";
            return false;
        }
        size_t remaining = (size_t)st.st_size;
        int dst_fd = fileno(dst);
        while (remaining > 0) {
            ssize_t n = copy_file_range(src_fd, nullptr, dst_fd, nullptr, remaining, 0);
            if (n <= 0) {
                close(src_fd);
                std::cerr << "append_file: copy_file_range failed\n";
                return false;
            }
            remaining -= (size_t)n;
        }
        close(src_fd);
        return true;
    }
#endif
    FILE* f = fopen(src.c_str(), "rb");
    if (!f) {
        std::cerr << "append_file: cannot open " << src << "\n";
        return false;
    }
    char buf[1 << 20];  // 1 MB stack buffer
    size_t n;
    while ((n = fread(buf, 1, sizeof(buf), f)) > 0) {
        if (fwrite(buf, 1, n, dst) != n) {
            fclose(f);
            std::cerr << "append_file: write error\n";
            return false;
        }
    }
    fclose(f);
    return true;
}

// ---------------------------------------------------------------------------
// Write mode
// ---------------------------------------------------------------------------

static int write_tdf(
    const std::filesystem::path& output_dir,
    const std::vector<FrameEntry>& frame_index,
    std::span<const uint32_t> ms1_scans,
    std::span<const uint32_t> ms1_tofs,
    std::span<const uint32_t> ms1_ints,
    std::span<const uint32_t> ms2_scans,
    std::span<const uint32_t> ms2_tofs,
    std::span<const uint32_t> ms2_ints,
    uint32_t total_scans,
    bool verbose,
    size_t threads,
    int zstd_level,
    bool use_fread,
    std::vector<uint32_t>& ids,
    std::vector<uint64_t>& tims_ids,
    std::vector<uint32_t>& num_peaks,
    std::vector<uint32_t>& max_ints,
    std::vector<uint64_t>& sum_ints)
{
    size_t n_frames  = frame_index.size();
    size_t n_threads = std::min(threads, n_frames > 0 ? n_frames : size_t(1));

    // Allocate metadata arrays upfront (one entry per frame, indexed 0..n_frames-1).
    ids.assign(n_frames, 0);
    num_peaks.assign(n_frames, 0);
    max_ints.assign(n_frames, 0);
    tims_ids.assign(n_frames, 0);
    sum_ints.assign(n_frames, 0);

    // Per-thread slice boundaries and binary paths.
    std::vector<size_t> slice_starts(n_threads + 1);
    std::vector<std::filesystem::path> bin_paths(n_threads);
    for (size_t k = 0; k < n_threads; ++k) {
        slice_starts[k] = k * n_frames / n_threads;
        if (n_threads == 1) {
            std::filesystem::create_directories(output_dir);
            bin_paths[k] = output_dir / "analysis.tdf_bin";
        } else {
            auto tmp_dir = output_dir / ("_tmp_" + std::to_string(k));
            std::filesystem::create_directories(tmp_dir);
            bin_paths[k] = tmp_dir / "analysis.tdf_bin";
        }
    }
    slice_starts[n_threads] = n_frames;

    // Launch threads — each writes its slice to its own binary file and
    // stores metadata directly into the pre-allocated vectors.
    std::atomic<bool> had_error{false};
    {
        std::vector<std::thread> workers;
        workers.reserve(n_threads);
        for (size_t k = 0; k < n_threads; ++k) {
            workers.emplace_back([&, k]() {
                size_t beg = slice_starts[k];
                size_t end = slice_starts[k + 1];
                TdfWriter writer(
                    bin_paths[k], total_scans,
                    {ids.data()       + beg, end - beg},
                    {tims_ids.data()  + beg, end - beg},
                    {num_peaks.data() + beg, end - beg},
                    {max_ints.data()  + beg, end - beg},
                    {sum_ints.data()  + beg, end - beg},
                    verbose && n_threads == 1, zstd_level);
                for (size_t idx = beg; idx < end; ++idx) {
                    const FrameEntry& fe = frame_index[idx];
                    std::span<const uint32_t> scans, tofs, ints;
                    if (fe.source == MS1) {
                        scans = {ms1_scans.data() + fe.start, fe.end - fe.start};
                        tofs  = {ms1_tofs.data()  + fe.start, fe.end - fe.start};
                        ints  = {ms1_ints.data()  + fe.start, fe.end - fe.start};
                    } else if (fe.source == MS2) {
                        scans = {ms2_scans.data() + fe.start, fe.end - fe.start};
                        tofs  = {ms2_tofs.data()  + fe.start, fe.end - fe.start};
                        ints  = {ms2_ints.data()  + fe.start, fe.end - fe.start};
                    }
                    // EMPTY: all spans remain default-constructed (empty)
                    if (!writer.write_frame(fe.frame_id, scans, tofs, ints)) {
                        had_error.store(true, std::memory_order_relaxed);
                        return;
                    }
                }
            });
        }
        for (auto& t : workers) t.join();
    }

    if (had_error) return 1;

    if (n_threads > 1) {
        // Fix up TimsIds in memory (chunk-relative → global byte offsets).
        std::vector<uint64_t> offsets(n_threads, 0);
        for (size_t k = 1; k < n_threads; ++k)
            offsets[k] = offsets[k - 1] + (uint64_t)std::filesystem::file_size(bin_paths[k - 1]);
        for (size_t k = 0; k < n_threads; ++k) {
            for (size_t i = slice_starts[k]; i < slice_starts[k + 1]; ++i)
                tims_ids[i] += offsets[k];
        }

        // Concatenate per-thread binary files into the final analysis.tdf_bin.
        std::filesystem::create_directories(output_dir);
        auto out_bin = output_dir / "analysis.tdf_bin";
        FILE* out = fopen(out_bin.c_str(), "wb");
        if (!out) {
            std::cerr << "write_tdf: cannot create " << out_bin << "\n";
            return 1;
        }
        for (size_t k = 0; k < n_threads; ++k) {
            if (!append_file(out, bin_paths[k], use_fread)) {
                fclose(out);
                return 1;
            }
        }
        fclose(out);

        // Remove temp dirs.
        for (size_t k = 0; k < n_threads; ++k)
            std::filesystem::remove_all(bin_paths[k].parent_path());
    }

    if (verbose) std::cout << std::format("wrote {} frames to {}\n",
                            n_frames,
                            (output_dir / "analysis.tdf_bin").string());
    return 0;
}


// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------

int main(int argc, char** argv) {
    Config cfg = parse_args(argc, argv);

    // Open columns (conditionally — each source is optional)
    std::optional<MMappedData<uint32_t>> ms1_frames_mm, ms1_scans_mm, ms1_tofs_mm, ms1_ints_mm;
    std::span<const uint32_t> ms1_frames_sp, ms1_scans_sp, ms1_tofs_sp, ms1_ints_sp;
    if (cfg.ms1_path) {
        ms1_frames_mm.emplace(OpenColumn<uint32_t>(*cfg.ms1_path, "frame"));
        ms1_scans_mm .emplace(OpenColumn<uint32_t>(*cfg.ms1_path, "scan"));
        ms1_tofs_mm  .emplace(OpenColumn<uint32_t>(*cfg.ms1_path, "tof"));
        ms1_ints_mm  .emplace(OpenColumn<uint32_t>(*cfg.ms1_path, "intensity"));
        ms1_frames_sp = {ms1_frames_mm->data(), ms1_frames_mm->size()};
        ms1_scans_sp  = {ms1_scans_mm ->data(), ms1_scans_mm ->size()};
        ms1_tofs_sp   = {ms1_tofs_mm  ->data(), ms1_tofs_mm  ->size()};
        ms1_ints_sp   = {ms1_ints_mm  ->data(), ms1_ints_mm  ->size()};
    }

    std::optional<MMappedData<uint32_t>> ms2_frames_mm, ms2_scans_mm, ms2_tofs_mm, ms2_ints_mm;
    std::span<const uint32_t> ms2_frames_sp, ms2_scans_sp, ms2_tofs_sp, ms2_ints_sp;
    if (cfg.ms2_path) {
        ms2_frames_mm.emplace(OpenColumn<uint32_t>(*cfg.ms2_path, "frame"));
        ms2_scans_mm .emplace(OpenColumn<uint32_t>(*cfg.ms2_path, "scan"));
        ms2_tofs_mm  .emplace(OpenColumn<uint32_t>(*cfg.ms2_path, "tof"));
        ms2_ints_mm  .emplace(OpenColumn<uint32_t>(*cfg.ms2_path, "intensity"));
        ms2_frames_sp = {ms2_frames_mm->data(), ms2_frames_mm->size()};
        ms2_scans_sp  = {ms2_scans_mm ->data(), ms2_scans_mm ->size()};
        ms2_tofs_sp   = {ms2_tofs_mm  ->data(), ms2_tofs_mm  ->size()};
        ms2_ints_sp   = {ms2_ints_mm  ->data(), ms2_ints_mm  ->size()};
    }

    Source gap_source = EMPTY;
    if (!cfg.ms2_path.has_value() && cfg.ms1_path.has_value())  gap_source = MS2;
    if (!cfg.ms1_path.has_value() && cfg.ms2_path.has_value())  gap_source = MS1;
    std::vector<FrameEntry> frame_index = build_frame_index(
        ms1_frames_sp, ms2_frames_sp, cfg.max_frames, gap_source);

    if (cfg.verbose)
        print_frame_index(frame_index);

    // Derive row counts and total_scans from the capped index
    size_t ms1_events = 0, ms2_events = 0;
    for (const auto& fe : frame_index) {
        if (fe.source == MS1) ms1_events = std::max(ms1_events, fe.end);
        if (fe.source == MS2) ms2_events = std::max(ms2_events, fe.end);
    }
    if (cfg.verbose){
        std::cout << std::format("ms1 events used: {} / {}\n", ms1_events, ms1_frames_sp.size());
        std::cout << std::format("ms2 events used: {} / {}\n", ms2_events, ms2_frames_sp.size());
    }

    uint32_t max_scan = 0;
    for (size_t i = 0; i < ms1_events; ++i)
        if (ms1_scans_sp[i] > max_scan) max_scan = ms1_scans_sp[i];
    for (size_t i = 0; i < ms2_events; ++i)
        if (ms2_scans_sp[i] > max_scan) max_scan = ms2_scans_sp[i];
    uint32_t total_scans = (ms1_events + ms2_events > 0) ? max_scan + 1 : 0;
    if (cfg.verbose) std::cout << std::format("total_scans: {}\n", total_scans);

    // -----------------------------------------------------------------------
    // Dry-run: print events in the order they would be written
    // -----------------------------------------------------------------------
    if (!cfg.output_dir) {
        print_dry_run(frame_index,
            ms1_scans_sp, ms1_tofs_sp, ms1_ints_sp,
            ms2_scans_sp, ms2_tofs_sp, ms2_ints_sp);
        return 0;
    }

    std::vector<uint32_t> ids, num_peaks, max_ints;
    std::vector<uint64_t> tims_ids, sum_ints;

    int rc = write_tdf(
        *cfg.output_dir,
        frame_index,
        ms1_scans_sp, ms1_tofs_sp, ms1_ints_sp,
        ms2_scans_sp, ms2_tofs_sp, ms2_ints_sp,
        total_scans,
        cfg.verbose,
        cfg.threads,
        cfg.zstd_level,
        cfg.use_fread,
        ids, tims_ids, num_peaks, max_ints, sum_ints);
    if (rc != 0) return rc;

    if (cfg.tdf_src) {
        rc = write_analysis_tdf(
            *cfg.output_dir, *cfg.tdf_src,
            frame_index, ids, tims_ids, num_peaks, max_ints, sum_ints,
            total_scans, cfg.atdf_cfg);
        if (rc != 0) return rc;
    }

    if (cfg.write_metadata) {
        auto meta = Schema<uint32_t, uint64_t, uint32_t, uint32_t, uint64_t>(
            "Id", "TimsId", "NumPeaks", "MaxIntensity", "SummedIntensities"
        ).create_writer(*cfg.output_dir / "frames_metadata.mmappet");
        meta.write_rows(frame_index.size(),
            ids.data(), tims_ids.data(), num_peaks.data(), max_ints.data(), sum_ints.data());
    }

    return 0;
}
