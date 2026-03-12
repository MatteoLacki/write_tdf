#include "mmappet.h"
#include <zstd.h>
#include <cstdio>
#include <cstring>
#include <algorithm>
#include <iostream>
#include <filesystem>
#include <vector>


int main(int argc, char** argv) {
    if (argc != 3) {
        std::cerr << "Usage: pmsms2tdf <input.mmappet> <output_tdf_bin>\n";
        return 1;
    }

    std::filesystem::path input_path(argv[1]);
    std::filesystem::path output_tdf_bin(argv[2]);

    // 1. Open 4 columns
    auto frames_col      = OpenColumn<uint32_t>(input_path, "frame");
    auto scans_col       = OpenColumn<uint32_t>(input_path, "scan");
    auto tofs_col        = OpenColumn<uint32_t>(input_path, "tof");
    auto intensities_col = OpenColumn<uint32_t>(input_path, "intensity");

    size_t total_rows = frames_col.size();

    // 2. Compute total_scans = global max(scan) + 1
    uint32_t max_scan = 0;
    for (size_t i = 0; i < total_rows; ++i)
        if (scans_col[i] > max_scan) max_scan = scans_col[i];
    uint32_t total_scans = (total_rows > 0) ? max_scan + 1 : 0;

    // 3. Build frame group boundaries (linear scan, data pre-sorted)
    std::vector<size_t> frame_starts;
    frame_starts.reserve(1024);
    frame_starts.push_back(0);
    for (size_t i = 1; i < total_rows; ++i)
        if (frames_col[i] != frames_col[i-1])
            frame_starts.push_back(i);
    frame_starts.push_back(total_rows); // sentinel

    // Compute max frame size for buffer pre-allocation
    size_t max_frame_n = 0;
    for (size_t g = 0; g + 1 < frame_starts.size(); ++g) {
        size_t n = frame_starts[g+1] - frame_starts[g];
        if (n > max_frame_n) max_frame_n = n;
    }

    size_t max_back_size = (size_t)total_scans * 4 + 2 * max_frame_n * 4;
    std::vector<uint8_t> compress_buf(max_back_size > 0 ? ZSTD_compressBound(max_back_size) : 1024);

    // 4. Create output directory and open tdf_bin
    if (!output_tdf_bin.parent_path().empty())
        std::filesystem::create_directories(output_tdf_bin.parent_path());
    FILE* tdf_bin = fopen(output_tdf_bin.c_str(), "wb");
    if (!tdf_bin) {
        std::cerr << "Failed to open output file: " << output_tdf_bin << "\n";
        return 1;
    }

    // 5. Create metadata writer
    auto meta_writer = Schema<uint32_t, uint64_t, uint32_t, uint32_t, uint64_t>(
        "Id", "TimsId", "NumPeaks", "MaxIntensity", "SummedIntensities"
    ).create_writer(output_tdf_bin.parent_path() / "frames_metadata.mmappet");

    // Reusable per-frame buffers
    std::vector<uint32_t> peak_cnts(total_scans);
    std::vector<uint32_t> tof_deltas;
    std::vector<uint32_t> interleaved;
    std::vector<uint8_t>  back_data;
    std::vector<uint8_t>  real_data;

    // 6. Per-frame encoding loop
    for (size_t g = 0; g + 1 < frame_starts.size(); ++g) {
        size_t start = frame_starts[g], end = frame_starts[g+1];
        size_t n     = end - start;
        uint32_t frame_id = frames_col[start];

        // Record byte offset before writing this frame
        uint64_t tims_id = (uint64_t)ftello(tdf_bin);

        // Compute per-frame stats
        uint32_t max_int = 0;
        uint64_t sum_int = 0;
        for (size_t i = start; i < end; ++i) {
            uint32_t iv = intensities_col[i];
            if (iv > max_int) max_int = iv;
            sum_int += iv;
        }

        // Step 1: build peak_cnts header
        // peak_cnts[0]       = total_scans
        // peak_cnts[scan_id] = 2 * count(peaks with scan == scan_id - 1)  for scan_id in [1, total_scans)
        std::fill(peak_cnts.begin(), peak_cnts.end(), 0u);
        peak_cnts[0] = total_scans;
        for (size_t i = start; i < end; ++i) {
            uint32_t s = scans_col[i];
            if (s + 1 < total_scans)
                peak_cnts[s + 1]++;
        }
        for (uint32_t s = 1; s < total_scans; ++s)
            peak_cnts[s] *= 2;

        // Step 2: delta-encode TOFs per scan (copy — mmap is read-only)
        tof_deltas.resize(n);
        {
            uint32_t last_tof  = uint32_t(-1); // predecessor of first peak = -1 per reference
            uint32_t last_scan = uint32_t(-1); // force reset on first peak
            for (size_t i = 0; i < n; ++i) {
                uint32_t s = scans_col[start + i];
                if (s != last_scan) {
                    last_tof  = uint32_t(-1);
                    last_scan = s;
                }
                uint32_t val  = tofs_col[start + i];
                tof_deltas[i] = val - last_tof; // wraps correctly for first peak: val + 1
                last_tof      = val;
            }
        }

        // Step 3: interleave [tof_delta0, intensity0, tof_delta1, intensity1, ...]
        interleaved.resize(2 * n);
        for (size_t i = 0; i < n; ++i) {
            interleaved[2*i]     = tof_deltas[i];
            interleaved[2*i + 1] = intensities_col[start + i];
        }

        // Step 4: concat and 4-byte lane transpose
        // back_data = bytes(peak_cnts) || bytes(interleaved)
        // real_data[rd_idx] = back_data[bd_idx], bd_idx advances by 4, wrapping lane by lane
        size_t back_size = (size_t)total_scans * 4 + 2 * n * 4;
        back_data.resize(back_size);
        real_data.resize(back_size);
        memcpy(back_data.data(),                      peak_cnts.data(),  (size_t)total_scans * 4);
        memcpy(back_data.data() + (size_t)total_scans * 4, interleaved.data(), 2 * n * 4);

        {
            size_t reminder = 0;
            size_t bd_idx   = 0;
            for (size_t rd_idx = 0; rd_idx < back_size; ++rd_idx) {
                if (bd_idx >= back_size) {
                    ++reminder;
                    bd_idx = reminder;
                }
                real_data[rd_idx] = back_data[bd_idx];
                bd_idx += 4;
            }
        }

        // Step 5: compress with zstd
        size_t comp_size = ZSTD_compress(
            compress_buf.data(), compress_buf.size(),
            real_data.data(), back_size,
            1
        );
        if (ZSTD_isError(comp_size)) {
            std::cerr << "ZSTD compression error: " << ZSTD_getErrorName(comp_size) << "\n";
            fclose(tdf_bin);
            return 1;
        }

        // Step 6: write [block_size : uint32 LE][total_scans : uint32 LE][payload]
        // block_size = compressed_len + 8  (includes the 8-byte header itself)
        uint32_t block_size = (uint32_t)(comp_size + 8);
        fwrite(&block_size,   4, 1,         tdf_bin);
        fwrite(&total_scans,  4, 1,         tdf_bin);
        fwrite(compress_buf.data(), 1, comp_size, tdf_bin);

        meta_writer.write_row(frame_id, tims_id, (uint32_t)n, max_int, sum_int);
    }

    fclose(tdf_bin);
    return 0;
}
