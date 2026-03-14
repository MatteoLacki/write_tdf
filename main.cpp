#include "mmappet.h"
#include <zstd.h>
#include <cstdio>
#include <cstring>
#include <iostream>
#include <print>
#include <filesystem>
#include <vector>
#include <optional>
#include <algorithm>
#include <cstdlib>
#include <string_view>


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
    bool verbose = false;
};


// ---------------------------------------------------------------------------
// CLI
// ---------------------------------------------------------------------------

[[noreturn]] static void usage(const char* prog, int code) {
    std::cerr
        << "Usage: " << prog << " --ms1 <ms1.mmappet> --ms2 <ms2.mmappet>"
        << " [--output <output.d/>] [--max-frames N]\n"
        << "\n"
        << "  --ms1 PATH       input MS1 mmappet (frame/scan/tof/intensity)\n"
        << "  --ms2 PATH       input MS2 mmappet (frame/scan/tof/intensity)\n"
        << "  --output PATH    output .d directory; omit to print events instead of writing\n"
        << "  --max-frames N   cap on merged+gap-filled frame sequence (default: all)\n"
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
    bool verbose)
{
    size_t max_frame_event_cnt = 0;
    for (const auto& fe : frame_index) {
        size_t n_events = fe.end - fe.start;
        if (n_events > max_frame_event_cnt) max_frame_event_cnt = n_events;
    }
    size_t max_back_size = (size_t)total_scans * 4 + 2 * max_frame_event_cnt * 4;
    std::vector<uint8_t> compress_buf(max_back_size > 0 ? ZSTD_compressBound(max_back_size) : 1024);

    std::filesystem::create_directories(output_dir);
    std::filesystem::path output_tdf_bin = output_dir / "analysis.tdf_bin";
    std::unique_ptr<FILE, decltype(&fclose)> tdf_bin(
        fopen(output_tdf_bin.c_str(), "wb"), fclose);
    if (!tdf_bin) {
        std::cerr << "Failed to open output file: " << output_tdf_bin << "\n";
        return 1;
    }

    auto meta_writer = Schema<uint32_t, uint64_t, uint32_t, uint32_t, uint64_t>(
        "Id", "TimsId", "NumPeaks", "MaxIntensity", "SummedIntensities"
    ).create_writer(output_dir / "frames_metadata.mmappet");

    std::vector<uint32_t> peak_cnts(total_scans);
    std::vector<uint32_t> tof_deltas;
    std::vector<uint32_t> interleaved;
    std::vector<uint8_t>  back_data;
    std::vector<uint8_t>  real_data;

    for (const auto& fe : frame_index) {
        uint32_t frame_id     = fe.frame_id;
        size_t   n_events     = fe.end - fe.start;
        size_t   start        = fe.start;

        const MMappedData<uint32_t>* scans_col = nullptr;
        const MMappedData<uint32_t>* tofs_col  = nullptr;
        const MMappedData<uint32_t>* ints_col  = nullptr;
        if (fe.source == MS1) {
            scans_col = &ms1_scans; tofs_col = &ms1_tofs; ints_col = &ms1_ints;
        } else if (fe.source == MS2) {
            scans_col = &ms2_scans; tofs_col = &ms2_tofs; ints_col = &ms2_ints;
        }

        uint64_t tims_id = (uint64_t)ftello(tdf_bin.get());

        // Compute per-frame intensity statistics (max and sum) needed for the
        // metadata row. These are written later to frames_metadata.mmappet and
        // are used by downstream tools for filtering and normalization.
        uint32_t max_int = 0;
        uint64_t sum_int = 0;
        if (fe.source != EMPTY) {
            for (size_t i = start; i < fe.end; ++i) {
                uint32_t iv = (*ints_col)[i];
                if (iv > max_int) max_int = iv;
                sum_int += iv;
            }
        }

        // Build peak_cnts header
        std::fill(peak_cnts.begin(), peak_cnts.end(), 0u);
        peak_cnts[0] = total_scans;
        if (fe.source != EMPTY) {
            for (size_t i = start; i < fe.end; ++i) {
                uint32_t s = (*scans_col)[i];
                if (s + 1 < total_scans)
                    peak_cnts[s + 1]++;
            }
            for (uint32_t s = 1; s < total_scans; ++s)
                peak_cnts[s] *= 2;
        }

        // Delta-encode TOFs per scan
        tof_deltas.resize(n_events);
        if (fe.source != EMPTY) {
            uint32_t last_tof  = uint32_t(-1);
            uint32_t last_scan = uint32_t(-1);
            for (size_t i = 0; i < n_events; ++i) {
                uint32_t s = (*scans_col)[start + i];
                if (s != last_scan) { last_tof = uint32_t(-1); last_scan = s; }
                uint32_t val  = (*tofs_col)[start + i];
                tof_deltas[i] = val - last_tof;
                last_tof      = val;
            }
        }

        // Interleave [tof_delta, intensity, ...]
        interleaved.resize(2 * n_events);
        if (fe.source != EMPTY) {
            for (size_t i = 0; i < n_events; ++i) {
                interleaved[2*i]     = tof_deltas[i];
                interleaved[2*i + 1] = (*ints_col)[start + i];
            }
        }

        // Bruker's TDF binary layout stores data in a 4-byte lane-transposed
        // format: bytes at offsets 0, 4, 8, … form lane 0; offsets 1, 5, 9, …
        // form lane 1; etc. The loop below reorders back_data (peak_cnts header
        // followed by interleaved tof/intensity pairs) into that layout in
        // real_data, which is then ZSTD-compressed for better locality.
        // 4-byte lane transpose
        size_t back_size = (size_t)total_scans * 4 + 2 * n_events * 4;
        back_data.resize(back_size);
        real_data.resize(back_size);
        memcpy(back_data.data(), peak_cnts.data(), (size_t)total_scans * 4);
        if (n_events > 0)
            memcpy(back_data.data() + (size_t)total_scans * 4, interleaved.data(), 2 * n_events * 4);
        {
            size_t reminder = 0, bd_idx = 0;
            for (size_t rd_idx = 0; rd_idx < back_size; ++rd_idx) {
                if (bd_idx >= back_size) { ++reminder; bd_idx = reminder; }
                real_data[rd_idx] = back_data[bd_idx];
                bd_idx += 4;
            }
        }

        // ZSTD compress
        size_t comp_size = ZSTD_compress(
            compress_buf.data(), compress_buf.size(), real_data.data(), back_size, 1);
        if (ZSTD_isError(comp_size)) {
            std::cerr << "ZSTD compression error: " << ZSTD_getErrorName(comp_size) << "\n";
            return 1;
        }

        // Write block
        uint32_t block_size = (uint32_t)(comp_size + 8);
        fwrite(&block_size,  4, 1, tdf_bin.get());
        fwrite(&total_scans, 4, 1, tdf_bin.get());
        fwrite(compress_buf.data(), 1, comp_size, tdf_bin.get());

        meta_writer.write_row(frame_id, tims_id, (uint32_t)n_events, max_int, sum_int);

        if (verbose) {
            const char* src_str = (fe.source == MS1) ? "ms1" : (fe.source == MS2) ? "ms2" : "empty";
            std::print("frame {}  src={}  n_events={}  tims_id={}  max_int={}  sum_int={}\n",
                       frame_id, src_str, n_events, tims_id, max_int, sum_int);
        }
    }

    std::print("wrote {} frames to {}\n", frame_index.size(), output_tdf_bin.string());
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
    std::print("ms1 events used: {} / {}\n", ms1_events, ms1_frames.size());
    std::print("ms2 events used: {} / {}\n", ms2_events, ms2_frames.size());

    uint32_t max_scan = 0;
    for (size_t i = 0; i < ms1_events; ++i)
        if (ms1_scans[i] > max_scan) max_scan = ms1_scans[i];
    for (size_t i = 0; i < ms2_events; ++i)
        if (ms2_scans[i] > max_scan) max_scan = ms2_scans[i];
    uint32_t total_scans = (ms1_events + ms2_events > 0) ? max_scan + 1 : 0;
    std::print("total_scans: {}\n", total_scans);

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
        cfg.verbose);
}
