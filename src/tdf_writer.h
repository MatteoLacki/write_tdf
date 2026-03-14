#pragma once
#include <zstd.h>
#include <cstdio>
#include <cstring>
#include <filesystem>
#include <span>
#include <vector>
#include <iostream>
#include <print>
#include <cassert>

class TdfWriter {
public:
    // total_scans: instrument scan count, fixed for the whole acquisition.
    // Must be known upfront because it determines the peak_cnts header size
    // written into every frame block.
    //
    // The five out_* spans must each have length == number of frames this
    // writer will write. Metadata is stored directly into those spans;
    // the caller owns the backing storage and writes the mmappet after all
    // threads have finished.
    TdfWriter(const std::filesystem::path& tdf_bin_path,
              uint32_t total_scans,
              std::span<uint32_t> out_ids,
              std::span<uint64_t> out_tims_ids,
              std::span<uint32_t> out_num_peaks,
              std::span<uint32_t> out_max_ints,
              std::span<uint64_t> out_sum_ints,
              bool verbose = false,
              int  zstd_level = 1)
        : total_scans_(total_scans)
        , verbose_(verbose)
        , zstd_level_(zstd_level)
        , out_ids_(out_ids)
        , out_tims_ids_(out_tims_ids)
        , out_num_peaks_(out_num_peaks)
        , out_max_ints_(out_max_ints)
        , out_sum_ints_(out_sum_ints)
        , tdf_bin_(nullptr, fclose)
        , peak_cnts_(total_scans)
    {
        std::filesystem::create_directories(tdf_bin_path.parent_path());
        tdf_bin_.reset(fopen(tdf_bin_path.c_str(), "wb"));
        if (!tdf_bin_) {
            throw std::runtime_error("Failed to open output file: " + tdf_bin_path.string());
        }
    }

    // Non-copyable (owns FILE*)
    TdfWriter(const TdfWriter&) = delete;
    TdfWriter& operator=(const TdfWriter&) = delete;

    // Write one frame. All three spans must have the same length.
    // Pass empty spans to write an empty (gap-fill) frame.
    // Returns false on ZSTD or I/O error.
    bool write_frame(uint32_t frame_id,
                     std::span<const uint32_t> scans,
                     std::span<const uint32_t> tofs,
                     std::span<const uint32_t> intensities)
    {
        assert(scans.size() == tofs.size() && scans.size() == intensities.size());
        size_t n_events = scans.size();
        bool is_empty = (n_events == 0);

        uint64_t tims_id = (uint64_t)ftello(tdf_bin_.get());

        // Intensity statistics
        uint32_t max_int = 0;
        uint64_t sum_int = 0;
        for (size_t i = 0; i < n_events; ++i) {
            uint32_t iv = intensities[i];
            if (iv > max_int) max_int = iv;
            sum_int += iv;
        }

        // Build peak_cnts header
        std::fill(peak_cnts_.begin(), peak_cnts_.end(), 0u);
        peak_cnts_[0] = total_scans_;
        if (!is_empty) {
            for (size_t i = 0; i < n_events; ++i) {
                uint32_t s = scans[i];
                if (s + 1 < total_scans_)
                    peak_cnts_[s + 1]++;
            }
            for (uint32_t s = 1; s < total_scans_; ++s)
                peak_cnts_[s] *= 2;
        }

        // Delta-encode TOFs per scan
        tof_deltas_.resize(n_events);
        if (!is_empty) {
            uint32_t last_tof  = uint32_t(-1);
            uint32_t last_scan = uint32_t(-1);
            for (size_t i = 0; i < n_events; ++i) {
                uint32_t s = scans[i];
                if (s != last_scan) { last_tof = uint32_t(-1); last_scan = s; }
                uint32_t val  = tofs[i];
                tof_deltas_[i] = val - last_tof;
                last_tof       = val;
            }
        }

        // Interleave [tof_delta, intensity, ...]
        interleaved_.resize(2 * n_events);
        for (size_t i = 0; i < n_events; ++i) {
            interleaved_[2*i]     = tof_deltas_[i];
            interleaved_[2*i + 1] = intensities[i];
        }

        // 4-byte lane transpose into real_data
        size_t back_size = (size_t)total_scans_ * 4 + 2 * n_events * 4;
        back_data_.resize(back_size);
        real_data_.resize(back_size);
        memcpy(back_data_.data(), peak_cnts_.data(), (size_t)total_scans_ * 4);
        if (n_events > 0)
            memcpy(back_data_.data() + (size_t)total_scans_ * 4, interleaved_.data(), 2 * n_events * 4);
        {
            size_t reminder = 0, bd_idx = 0;
            for (size_t rd_idx = 0; rd_idx < back_size; ++rd_idx) {
                if (bd_idx >= back_size) { ++reminder; bd_idx = reminder; }
                real_data_[rd_idx] = back_data_[bd_idx];
                bd_idx += 4;
            }
        }

        // ZSTD compress — grow buffer dynamically per frame
        size_t compress_bound = ZSTD_compressBound(back_size);
        if (compress_buf_.size() < compress_bound)
            compress_buf_.resize(compress_bound);

        size_t comp_size = ZSTD_compress(
            compress_buf_.data(), compress_buf_.size(), real_data_.data(), back_size, zstd_level_);
        if (ZSTD_isError(comp_size)) {
            std::cerr << "ZSTD compression error: " << ZSTD_getErrorName(comp_size) << "\n";
            return false;
        }

        // Write block
        uint32_t block_size = (uint32_t)(comp_size + 8);
        fwrite(&block_size,  4, 1, tdf_bin_.get());
        fwrite(&total_scans_, 4, 1, tdf_bin_.get());
        fwrite(compress_buf_.data(), 1, comp_size, tdf_bin_.get());

        // Store metadata into caller-owned spans
        out_ids_[frames_written_]       = frame_id;
        out_tims_ids_[frames_written_]  = tims_id;
        out_num_peaks_[frames_written_] = (uint32_t)n_events;
        out_max_ints_[frames_written_]  = max_int;
        out_sum_ints_[frames_written_]  = sum_int;

        if (verbose_) {
            std::print("frame {}  n_events={}  tims_id={}  max_int={}  sum_int={}\n",
                       frame_id, n_events, tims_id, max_int, sum_int);
        }

        ++frames_written_;
        return true;
    }

    size_t frames_written() const { return frames_written_; }

private:
    uint32_t total_scans_;
    bool verbose_;
    int zstd_level_;

    // Caller-owned metadata spans (one slot per frame this writer handles)
    std::span<uint32_t> out_ids_;
    std::span<uint64_t> out_tims_ids_;
    std::span<uint32_t> out_num_peaks_;
    std::span<uint32_t> out_max_ints_;
    std::span<uint64_t> out_sum_ints_;

    std::unique_ptr<FILE, decltype(&fclose)> tdf_bin_;

    // Reusable scratch buffers — grow as needed, never shrink
    std::vector<uint32_t> peak_cnts_;
    std::vector<uint32_t> tof_deltas_;
    std::vector<uint32_t> interleaved_;
    std::vector<uint8_t>  back_data_;
    std::vector<uint8_t>  real_data_;
    std::vector<uint8_t>  compress_buf_;

    size_t frames_written_ = 0;
};
