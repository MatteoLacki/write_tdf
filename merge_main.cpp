// mmappet-merge-dedup — multithreaded K-way merge of per-cluster sorted runs.
//
// INPUT
//   An mmappet produced by massimo_cpp with columns:
//     ClusterID  frame  scan  tof  intensity  (all uint32)
//   Each contiguous block of equal ClusterID values is a "run". massimo_cpp
//   sorts events within each run by (frame, scan, tof) before writing, so every
//   run is a locally sorted sequence.
//
// OUTPUT
//   An mmappet with columns: frame  scan  tof  intensity  (all uint32)
//   Globally sorted by (frame, scan, tof) and deduplicated — ready for pmsms2tdf.
//   Events sharing the same (frame, scan, tof) triplet across different clusters
//   are collapsed into one row with their intensities summed.
//
// ALGORITHM — single-threaded baseline
//   A tournament tree (winner tree) performs a K-way merge of the K sorted runs
//   in O(N log K) time.  At each step the tree yields the globally smallest
//   (frame, scan, tof); adjacent identical triplets are accumulated into one
//   output row.
//
// MULTITHREADING — frame-range partitioning
//   The global frame ID space [global_min, global_max] is divided into T
//   non-overlapping ranges, one per thread.  Each thread independently:
//
//   1. Clips every cluster run to its frame range using two lower_bound calls
//      on the (sorted) frame column — O(K log(N/K)) total, no locking needed.
//      A cluster whose events span multiple frame ranges contributes a sub-cursor
//      to each thread that covers part of its range; a cluster with no events in
//      a thread's range is simply skipped.
//
//   2. Builds its own tournament tree over the clipped sub-cursors.  Because the
//      sub-cursors contain only events within [flo, fhi), the tree never sees an
//      out-of-range event.
//
//   3. Runs the merge + dedup loop and writes results to a private temp directory.
//
//   After all threads finish the main thread concatenates the per-thread column
//   files in order (t=0, 1, ..., T-1) to produce the final output.
//
//   Correctness: two events can only be duplicates if they share the same frame ID,
//   so they always land in the same thread — no cross-thread deduplication needed.

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <filesystem>
#include <iostream>
#include <limits>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include "mmappet.h"
#include "loser_tree.h"

namespace fs = std::filesystem;

// ---------------------------------------------------------------------------
// File helpers
// ---------------------------------------------------------------------------

// Append contents of file src to already-open FILE* dst.
// Uses copy_file_range (kernel-space) on Linux; falls back to fread/fwrite.
static bool append_file(FILE* dst, const fs::path& src)
{
#ifdef __linux__
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
#else
    FILE* f = fopen(src.c_str(), "rb");
    if (!f) {
        std::cerr << "append_file: cannot open " << src << "\n";
        return false;
    }
    char buf[1 << 20];
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
#endif
}

// ---------------------------------------------------------------------------
// Core helpers
// ---------------------------------------------------------------------------

// Returns [start, end) for each contiguous block of equal ClusterID values.
static std::vector<std::pair<size_t, size_t>>
find_cluster_runs(const uint32_t* ids, size_t n)
{
    std::vector<std::pair<size_t, size_t>> runs;
    size_t i = 0;
    while (i < n) {
        size_t j = i + 1;
        while (j < n && ids[j] == ids[i]) ++j;
        runs.push_back({i, j});
        i = j;
    }
    return runs;
}

// Clip run [start, end) to the frame range [frame_begin, frame_end).
// Exploits the fact that frames within a run are sorted ascending.
static std::pair<size_t, size_t>
clip_run(const uint32_t* frames, size_t start, size_t end,
         uint32_t frame_begin, uint32_t frame_end)
{
    size_t clip_begin = (size_t)(std::lower_bound(frames + start, frames + end, frame_begin) - frames);
    size_t clip_end   = (size_t)(std::lower_bound(frames + start, frames + end, frame_end)   - frames);
    return {clip_begin, clip_end};
}

// ---------------------------------------------------------------------------
// Per-thread worker
// ---------------------------------------------------------------------------

static void merge_worker(
    const uint32_t* frames,
    const uint32_t* scans,
    const uint32_t* tofs,
    const uint32_t* intensities,
    const std::vector<std::pair<size_t, size_t>>& runs,
    uint32_t frame_begin,
    uint32_t frame_end,
    const fs::path& tmp_dir)
{
    // Clip each run to [frame_begin, frame_end); collect non-empty sub-cursors.
    std::vector<RunCursor> cursors;
    for (auto [start, end] : runs) {
        auto [clip_begin, clip_end] = clip_run(frames, start, end, frame_begin, frame_end);
        if (clip_begin < clip_end)
            cursors.push_back({frames, scans, tofs, intensities, clip_begin, clip_end});
    }

    // Always create the writer so the column files exist for concatenation.
    auto writer = Schema<uint32_t, uint32_t, uint32_t, uint32_t>(
        "frame", "scan", "tof", "intensity"
    ).create_writer(tmp_dir);

    if (cursors.empty()) return;

    TournamentTree tree(std::move(cursors));

    constexpr size_t BATCH = 1 << 16;  // 64 K rows ≈ 1 MB per column buffer
    std::vector<uint32_t> buf_frame(BATCH), buf_scan(BATCH),
                          buf_tof(BATCH),   buf_int(BATCH);
    size_t n_buffered = 0;

    auto flush = [&]() {
        if (n_buffered == 0) return;
        writer.write_rows(n_buffered,
                          buf_frame.data(), buf_scan.data(),
                          buf_tof.data(),   buf_int.data());
        n_buffered = 0;
    };

    auto emit = [&](uint32_t f, uint32_t s, uint32_t t, uint32_t iv) {
        buf_frame[n_buffered] = f;
        buf_scan [n_buffered] = s;
        buf_tof  [n_buffered] = t;
        buf_int  [n_buffered] = iv;
        if (++n_buffered == BATCH) flush();
    };

    uint32_t prev_frame = 0, prev_scan = 0, prev_tof = 0;
    uint64_t accum = 0;
    bool first = true;

    while (!tree.empty()) {
        RunCursor& c = tree.top();
        uint32_t frame = c.frame(), scan = c.scan(), tof = c.tof(), iv = c.intensity();
        tree.pop();

        if (!first && frame == prev_frame && scan == prev_scan && tof == prev_tof) {
            accum += iv;
        } else {
            if (!first)
                emit(prev_frame, prev_scan, prev_tof,
                     (uint32_t)std::min(accum, (uint64_t)UINT32_MAX));
            prev_frame = frame;  prev_scan = scan;  prev_tof = tof;
            accum  = iv;
            first  = false;
        }
    }

    if (!first)
        emit(prev_frame, prev_scan, prev_tof,
             (uint32_t)std::min(accum, (uint64_t)UINT32_MAX));
    flush();
}

// ---------------------------------------------------------------------------
// main
// ---------------------------------------------------------------------------

int main(int argc, char* argv[])
{
    if (argc < 3) {
        std::cerr <<
            "Usage: mmappet-merge-dedup <input.mmappet> <output.mmappet> [--threads N]\n\n"
            "Merge K sorted runs (identified by ClusterID) into a single sorted,\n"
            "deduplicated mmappet with columns (frame, scan, tof, intensity).\n"
            "Input must have columns: ClusterID frame scan tof intensity (uint32).\n"
            "Duplicate (frame,scan,tof) triplets are collapsed by summing intensity.\n"
            "Default thread count: all hardware threads.\n";
        return 1;
    }

    fs::path input_path  = argv[1];
    fs::path output_path = argv[2];

    size_t n_threads = std::max(1u, std::thread::hardware_concurrency());
    for (int i = 3; i < argc; ++i) {
        if (std::string_view(argv[i]) == "--threads" && i + 1 < argc) {
            n_threads = std::stoul(argv[++i]);
            if (n_threads == 0) { std::cerr << "--threads must be >= 1\n"; return 1; }
        } else {
            std::cerr << "Unknown argument: " << argv[i] << "\n";
            return 1;
        }
    }

    // Open input columns.
    auto cluster_ids_mm = OpenColumn<uint32_t>(input_path, "ClusterID");
    auto frames_mm      = OpenColumn<uint32_t>(input_path, "frame");
    auto scans_mm       = OpenColumn<uint32_t>(input_path, "scan");
    auto tofs_mm        = OpenColumn<uint32_t>(input_path, "tof");
    auto intensities_mm = OpenColumn<uint32_t>(input_path, "intensity");

    size_t n = frames_mm.size();

    // Empty input → write empty output with the correct schema.
    if (n == 0) {
        Schema<uint32_t, uint32_t, uint32_t, uint32_t>(
            "frame", "scan", "tof", "intensity"
        ).create_writer(output_path);
        return 0;
    }

    const uint32_t* cluster_ids = cluster_ids_mm.data();
    const uint32_t* frames      = frames_mm.data();
    const uint32_t* scans       = scans_mm.data();
    const uint32_t* tofs        = tofs_mm.data();
    const uint32_t* intensities = intensities_mm.data();

    // One cursor boundary per contiguous ClusterID run.
    auto runs = find_cluster_runs(cluster_ids, n);

    // Find global frame range from per-run min/max — O(K), not O(N).
    uint32_t first_frame = UINT32_MAX, last_frame = 0;
    for (auto [start, end] : runs) {
        first_frame = std::min(first_frame, frames[start]);
        last_frame  = std::max(last_frame,  frames[end - 1]);
    }

    // Cap thread count to the number of distinct frames.
    n_threads = std::min(n_threads, (size_t)(last_frame - first_frame + 1));

    // Divide frame space [first_frame, last_frame] into n_threads equal ranges.
    uint64_t span = (uint64_t)last_frame - first_frame + 1;
    std::vector<uint32_t> frame_begins(n_threads), frame_ends(n_threads);
    for (size_t t = 0; t < n_threads; ++t) {
        frame_begins[t] = first_frame + (uint32_t)(t * span / n_threads);
        frame_ends[t]   = (t + 1 == n_threads)
                          ? last_frame + 1
                          : first_frame + (uint32_t)((t + 1) * span / n_threads);
    }

    // Launch one thread per frame range.
    std::vector<fs::path> tmp_dirs(n_threads);
    std::atomic<bool> had_error{false};
    {
        std::vector<std::thread> workers;
        workers.reserve(n_threads);
        for (size_t t = 0; t < n_threads; ++t) {
            tmp_dirs[t] = output_path / ("_tmp_" + std::to_string(t));
            workers.emplace_back([&, t]() {
                try {
                    merge_worker(frames, scans, tofs, intensities, runs,
                                 frame_begins[t], frame_ends[t], tmp_dirs[t]);
                } catch (const std::exception& e) {
                    std::cerr << "Thread " << t << " error: " << e.what() << "\n";
                    had_error.store(true, std::memory_order_relaxed);
                }
            });
        }
        for (auto& w : workers) w.join();
    }

    if (had_error) return 1;

    // Concatenate per-thread column files into the final output directory.
    fs::create_directories(output_path);
    Schema<uint32_t, uint32_t, uint32_t, uint32_t>(
        "frame", "scan", "tof", "intensity"
    ).write_schema_file(output_path / "schema.txt");

    for (int col = 0; col < 4; ++col) {
        fs::path out_col = output_path / (std::to_string(col) + ".bin");
        FILE* out = fopen(out_col.c_str(), "wb");
        if (!out) {
            std::cerr << "Cannot create " << out_col << "\n";
            return 1;
        }
        for (size_t t = 0; t < n_threads; ++t) {
            if (!append_file(out, tmp_dirs[t] / (std::to_string(col) + ".bin"))) {
                fclose(out);
                return 1;
            }
        }
        fclose(out);
    }

    // Clean up temp directories.
    for (auto& d : tmp_dirs) fs::remove_all(d);

    return 0;
}
