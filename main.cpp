#include "tdf_writer.h"
#include "mmappet.h"
#include <iostream>
#include <print>
#include <filesystem>
#include <vector>
#include <optional>
#include <algorithm>
#include <cstdlib>
#include <string_view>
#include <thread>
#include <atomic>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>


// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

enum Source : int8_t { MS1 = 0, MS2 = 1, EMPTY = -1 };
struct FrameEntry { uint32_t frame_id; Source source; size_t start; size_t end; };

struct Config {
    std::filesystem::path ms1_path;
    std::filesystem::path ms2_path;
    std::optional<std::filesystem::path> output_dir;  // absent = dry-run
    size_t max_frames = SIZE_MAX;
    size_t threads = 1;
    int zstd_level = 1;
    bool verbose = false;
};


// ---------------------------------------------------------------------------
// CLI
// ---------------------------------------------------------------------------

[[noreturn]] static void usage(const char* prog, int code) {
    std::cerr
        << "Usage: " << prog << " --ms1 <ms1.mmappet> --ms2 <ms2.mmappet>"
        << " [--output <output.d/>] [--max-frames N] [--threads N]\n"
        << "\n"
        << "  --ms1 PATH       input MS1 mmappet (frame/scan/tof/intensity)\n"
        << "  --ms2 PATH       input MS2 mmappet (frame/scan/tof/intensity)\n"
        << "  --output PATH    output .d directory; omit to print events instead of writing\n"
        << "  --max-frames N   cap on merged+gap-filled frame sequence (default: all)\n"
        << "  --threads N      number of parallel writer threads (default: 1)\n"
        << "  --zstd-level N   ZSTD compression level 1-22 (default: 1)\n"
        << "  --verbose        print first/last 10 entries of the frame index\n";
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
        } else if (arg == "--help" || arg == "-h") {
            usage(argv[0], 0);
        } else {
            std::cerr << "Error: unknown option " << arg << "\n";
            usage(argv[0], 1);
        }
    }

    if (!have_ms1 || !have_ms2) {
        std::cerr << "Error: --ms1 and --ms2 are required\n";
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
    size_t max_frames)
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
            idx.push_back({fid, EMPTY, 0, 0});

        if (idx.size() < max_frames)
            idx.push_back(fe);
        take_ms1 ? ++i : ++j;
    }

    std::print("frames built: {}\n", idx.size());
    return idx;
}


// ---------------------------------------------------------------------------
// Printing helpers
// ---------------------------------------------------------------------------

static void print_frame_index(const std::vector<FrameEntry>& frame_index) {
    size_t n = frame_index.size();
    size_t head = std::min(n, size_t(10));
    size_t tail_start = std::max(head, n > 10 ? n - 10 : n);
    std::print("frame_index:\n");
    for (size_t k = 0; k < head; ++k) {
        const FrameEntry& fe = frame_index[k];
        const char* src = (fe.source == MS1) ? "ms1" : (fe.source == MS2) ? "ms2" : "empty";
        std::print("  [{}] frame={}  src={}  rows=[{},{})\n", k, fe.frame_id, src, fe.start, fe.end);
    }
    if (tail_start > head) std::print("  ...\n");
    for (size_t k = tail_start; k < n; ++k) {
        const FrameEntry& fe = frame_index[k];
        const char* src = (fe.source == MS1) ? "ms1" : (fe.source == MS2) ? "ms2" : "empty";
        std::print("  [{}] frame={}  src={}  rows=[{},{})\n", k, fe.frame_id, src, fe.start, fe.end);
    }
}

static void print_dry_run(
    const std::vector<FrameEntry>& frame_index,
    const MMappedData<uint32_t>& ms1_scans,
    const MMappedData<uint32_t>& ms1_tofs,
    const MMappedData<uint32_t>& ms1_ints,
    const MMappedData<uint32_t>& ms2_scans,
    const MMappedData<uint32_t>& ms2_tofs,
    const MMappedData<uint32_t>& ms2_ints)
{
    std::print("frame\tscan\ttof\tintensity\n");
    for (const auto& fe : frame_index) {
        if (fe.source == EMPTY) continue;
        const auto* scans_col = (fe.source == MS1) ? &ms1_scans : &ms2_scans;
        const auto* tofs_col  = (fe.source == MS1) ? &ms1_tofs  : &ms2_tofs;
        const auto* ints_col  = (fe.source == MS1) ? &ms1_ints  : &ms2_ints;
        for (size_t i = fe.start; i < fe.end; ++i)
            std::print("{}\t{}\t{}\t{}\n", fe.frame_id, (*scans_col)[i], (*tofs_col)[i], (*ints_col)[i]);
    }
}


// ---------------------------------------------------------------------------
// Merge helpers
// ---------------------------------------------------------------------------

// Append contents of file `src` to already-open FILE* `dst`.
static bool append_file(FILE* dst, const std::filesystem::path& src) {
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

// Add `byte_offset` to every uint64_t in `meta_dir/1.bin` (the TimsId column).
static bool fixup_tims_ids(const std::filesystem::path& meta_dir, uint64_t byte_offset) {
    if (byte_offset == 0) return true;
    auto path = meta_dir / "1.bin";
    int fd = open(path.c_str(), O_RDWR);
    if (fd < 0) {
        std::cerr << "fixup_tims_ids: open failed: " << path << "\n";
        return false;
    }
    struct stat st;
    fstat(fd, &st);
    size_t sz = (size_t)st.st_size;
    if (sz == 0) { close(fd); return true; }
    void* p = mmap(nullptr, sz, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    close(fd);
    if (p == MAP_FAILED) {
        std::cerr << "fixup_tims_ids: mmap failed\n";
        return false;
    }
    auto* data = static_cast<uint64_t*>(p);
    size_t count = sz / sizeof(uint64_t);
    for (size_t i = 0; i < count; ++i)
        data[i] += byte_offset;
    msync(p, sz, MS_SYNC);
    munmap(p, sz);
    return true;
}

// Concatenate `chunk_meta_dirs[k]/col_idx.bin` into `dst_dir/col_idx.bin`.
static bool merge_column(const std::filesystem::path& dst_dir,
                         int col_idx,
                         const std::vector<std::filesystem::path>& chunk_meta_dirs)
{
    auto dst_path = dst_dir / (std::to_string(col_idx) + ".bin");
    FILE* out = fopen(dst_path.c_str(), "wb");
    if (!out) {
        std::cerr << "merge_column: cannot create " << dst_path << "\n";
        return false;
    }
    for (const auto& src_dir : chunk_meta_dirs) {
        auto src_path = src_dir / (std::to_string(col_idx) + ".bin");
        if (!append_file(out, src_path)) {
            fclose(out);
            return false;
        }
    }
    fclose(out);
    return true;
}


// ---------------------------------------------------------------------------
// Write mode
// ---------------------------------------------------------------------------

static int write_tdf(
    const std::filesystem::path& output_dir,
    const std::vector<FrameEntry>& frame_index,
    const MMappedData<uint32_t>& ms1_scans,
    const MMappedData<uint32_t>& ms1_tofs,
    const MMappedData<uint32_t>& ms1_ints,
    const MMappedData<uint32_t>& ms2_scans,
    const MMappedData<uint32_t>& ms2_tofs,
    const MMappedData<uint32_t>& ms2_ints,
    uint32_t total_scans,
    bool verbose,
    size_t threads,
    int zstd_level)
{
    size_t n_frames = frame_index.size();
    size_t n_threads = std::min(threads, n_frames > 0 ? n_frames : size_t(1));

    // Build chunk directories and per-thread frame slices.
    std::vector<std::filesystem::path> chunk_dirs(n_threads);
    std::vector<size_t> slice_starts(n_threads + 1);
    for (size_t k = 0; k < n_threads; ++k) {
        chunk_dirs[k] = output_dir / ("_tmp_" + std::to_string(k));
        slice_starts[k] = k * n_frames / n_threads;
    }
    slice_starts[n_threads] = n_frames;

    // Launch threads — each writes its slice to its own chunk dir.
    std::atomic<bool> had_error{false};
    std::vector<size_t> frames_written(n_threads, 0);
    {
        std::vector<std::thread> workers;
        workers.reserve(n_threads);
        for (size_t k = 0; k < n_threads; ++k) {
            workers.emplace_back([&, k]() {
                // verbose only when single-threaded (avoids interleaved output)
                TdfWriter writer(chunk_dirs[k], total_scans, verbose && n_threads == 1, zstd_level);
                size_t beg = slice_starts[k];
                size_t end = slice_starts[k + 1];
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
                frames_written[k] = writer.frames_written();
            });
        }
        for (auto& t : workers) t.join();
    }

    if (had_error) return 1;

    size_t total_written = 0;
    for (size_t k = 0; k < n_threads; ++k) total_written += frames_written[k];

    if (n_threads == 1) {
        // Fast path: just move the single chunk into the output dir.
        std::filesystem::create_directories(output_dir);
        std::filesystem::rename(chunk_dirs[0] / "analysis.tdf_bin",
                                output_dir / "analysis.tdf_bin");
        std::filesystem::rename(chunk_dirs[0] / "frames_metadata.mmappet",
                                output_dir / "frames_metadata.mmappet");
        std::filesystem::remove(chunk_dirs[0]);
    } else {
        // Compute cumulative byte offsets from chunk file sizes.
        std::vector<uint64_t> offsets(n_threads, 0);
        for (size_t k = 1; k < n_threads; ++k) {
            offsets[k] = offsets[k - 1] +
                         (uint64_t)std::filesystem::file_size(chunk_dirs[k - 1] / "analysis.tdf_bin");
        }

        // Fix up TimsId columns in place (chunk-relative → global).
        for (size_t k = 0; k < n_threads; ++k) {
            if (!fixup_tims_ids(chunk_dirs[k] / "frames_metadata.mmappet", offsets[k]))
                return 1;
        }

        // Concatenate analysis.tdf_bin files.
        std::filesystem::create_directories(output_dir);
        {
            auto out_bin = output_dir / "analysis.tdf_bin";
            FILE* out = fopen(out_bin.c_str(), "wb");
            if (!out) {
                std::cerr << "write_tdf: cannot create " << out_bin << "\n";
                return 1;
            }
            for (size_t k = 0; k < n_threads; ++k) {
                if (!append_file(out, chunk_dirs[k] / "analysis.tdf_bin")) {
                    fclose(out);
                    return 1;
                }
            }
            fclose(out);
        }

        // Merge frames_metadata.mmappet columns.
        auto dst_meta = output_dir / "frames_metadata.mmappet";
        std::filesystem::create_directories(dst_meta);

        // Copy schema.txt from first chunk (all chunks have identical schema).
        std::filesystem::copy_file(
            chunk_dirs[0] / "frames_metadata.mmappet" / "schema.txt",
            dst_meta / "schema.txt",
            std::filesystem::copy_options::overwrite_existing);

        // Collect chunk meta dirs for column merge.
        std::vector<std::filesystem::path> chunk_meta_dirs(n_threads);
        for (size_t k = 0; k < n_threads; ++k)
            chunk_meta_dirs[k] = chunk_dirs[k] / "frames_metadata.mmappet";

        // 5 columns: Id(0), TimsId(1), NumPeaks(2), MaxIntensity(3), SummedIntensities(4)
        for (int col = 0; col < 5; ++col) {
            if (!merge_column(dst_meta, col, chunk_meta_dirs))
                return 1;
        }

        // Clean up temp dirs.
        for (size_t k = 0; k < n_threads; ++k)
            std::filesystem::remove_all(chunk_dirs[k]);
    }

    if(verbose) std::print("wrote {} frames to {}\n",
                           total_written,
                           (output_dir / "analysis.tdf_bin").string());
    return 0;
}


// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------

int main(int argc, char** argv) {
    Config cfg = parse_args(argc, argv);

    // Open columns
    auto ms1_frames = OpenColumn<uint32_t>(cfg.ms1_path, "frame");
    auto ms1_scans  = OpenColumn<uint32_t>(cfg.ms1_path, "scan");
    auto ms1_tofs   = OpenColumn<uint32_t>(cfg.ms1_path, "tof");
    auto ms1_ints   = OpenColumn<uint32_t>(cfg.ms1_path, "intensity");

    auto ms2_frames = OpenColumn<uint32_t>(cfg.ms2_path, "frame");
    auto ms2_scans  = OpenColumn<uint32_t>(cfg.ms2_path, "scan");
    auto ms2_tofs   = OpenColumn<uint32_t>(cfg.ms2_path, "tof");
    auto ms2_ints   = OpenColumn<uint32_t>(cfg.ms2_path, "intensity");

    std::vector<FrameEntry> frame_index = build_frame_index(
        {ms1_frames.data(), ms1_frames.size()},
        {ms2_frames.data(), ms2_frames.size()},
        cfg.max_frames);

    if (cfg.verbose)
        print_frame_index(frame_index);

    // Derive row counts and total_scans from the capped index
    size_t ms1_events = 0, ms2_events = 0;
    for (const auto& fe : frame_index) {
        if (fe.source == MS1) ms1_events = std::max(ms1_events, fe.end);
        if (fe.source == MS2) ms2_events = std::max(ms2_events, fe.end);
    }
    if (cfg.verbose){
        std::print("ms1 events used: {} / {}\n", ms1_events, ms1_frames.size());
        std::print("ms2 events used: {} / {}\n", ms2_events, ms2_frames.size());
    }

    uint32_t max_scan = 0;
    for (size_t i = 0; i < ms1_events; ++i)
        if (ms1_scans[i] > max_scan) max_scan = ms1_scans[i];
    for (size_t i = 0; i < ms2_events; ++i)
        if (ms2_scans[i] > max_scan) max_scan = ms2_scans[i];
    uint32_t total_scans = (ms1_events + ms2_events > 0) ? max_scan + 1 : 0;
    if (cfg.verbose) std::print("total_scans: {}\n", total_scans);

    // -----------------------------------------------------------------------
    // Dry-run: print events in the order they would be written
    // -----------------------------------------------------------------------
    if (!cfg.output_dir) {
        print_dry_run(frame_index, ms1_scans, ms1_tofs, ms1_ints, ms2_scans, ms2_tofs, ms2_ints);
        return 0;
    }

    return write_tdf(
        *cfg.output_dir,
        frame_index,
        ms1_scans, ms1_tofs, ms1_ints,
        ms2_scans, ms2_tofs, ms2_ints,
        total_scans,
        cfg.verbose,
        cfg.threads,
        cfg.zstd_level);
}
