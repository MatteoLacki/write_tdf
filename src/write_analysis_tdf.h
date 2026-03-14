// write_analysis_tdf.h — header-only SQLite writer for analysis.tdf
//
// IMPORTANT: FrameEntry, Source, MS1, MS2, EMPTY must be defined before
// including this header (they live in main.cpp).
#pragma once

#include <sqlite3.h>
#include <filesystem>
#include <vector>
#include <optional>
#include <array>
#include <set>
#include <print>

#include "mmappet.h"

// ---------------------------------------------------------------------------
// Config for write_analysis_tdf()
// ---------------------------------------------------------------------------

struct AnalysisTdfConfig {
    std::optional<double> T1;
    std::optional<double> T2;
    std::optional<double> pressure;
    double accumulation_time_ms   = 100.0;
    double ramp_time_ms           = 100.0;
    int    denoised               = 0;
    int    mz_calibration         = 1;
    char   polarity               = '+';
    int    scan_mode              = 9;
    std::optional<std::array<double, 10>>  calib_params;
    std::optional<std::filesystem::path>   dia_windows_path;
};

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

namespace atdf_detail {

// One stored FrameProperties row: property id + copied sqlite3_value.
// The value is owned (allocated via sqlite3_value_dup); free with sqlite3_value_free.
struct PropVal {
    int           property;
    sqlite3_value* val;
};

// Read all (Property, Value) pairs for `frame_id` from FrameProperties.
// Caller must free each val with sqlite3_value_free().
inline std::vector<PropVal> read_frame_props(sqlite3* db, int frame_id) {
    std::vector<PropVal> props;
    sqlite3_stmt* s = nullptr;
    if (sqlite3_prepare_v2(db,
            "SELECT Property, Value FROM FrameProperties WHERE Frame=?",
            -1, &s, nullptr) != SQLITE_OK)
        return props;
    sqlite3_bind_int(s, 1, frame_id);
    while (sqlite3_step(s) == SQLITE_ROW) {
        int prop = sqlite3_column_int(s, 0);
        sqlite3_value* val = sqlite3_value_dup(sqlite3_column_value(s, 1));
        if (val) props.push_back({prop, val});
    }
    sqlite3_finalize(s);
    return props;
}

inline void free_props(std::vector<PropVal>& v) {
    for (auto& p : v) sqlite3_value_free(p.val);
    v.clear();
}

// RAII SQLite connection closer.
struct DBGuard {
    sqlite3* db;
    explicit DBGuard(sqlite3* d) : db(d) {}
    ~DBGuard() { if (db) sqlite3_close(db); }
    DBGuard(const DBGuard&)            = delete;
    DBGuard& operator=(const DBGuard&) = delete;
};

} // namespace atdf_detail

// ---------------------------------------------------------------------------
// Main function
// ---------------------------------------------------------------------------

// Copy `tdf_src` to `output_dir/analysis.tdf`, open it, delete dynamic table
// rows (Frames, FrameProperties, DiaFrameMsMsInfo, Segments), and repopulate
// them from frame_index + metadata arrays in one SQLite transaction.
//
// Returns 0 on success, 1 on any error.
inline int write_analysis_tdf(
    const std::filesystem::path&   output_dir,
    const std::filesystem::path&   tdf_src,
    const std::vector<FrameEntry>& frame_index,
    const std::vector<uint32_t>&   ids,
    const std::vector<uint64_t>&   tims_ids,
    const std::vector<uint32_t>&   num_peaks,
    const std::vector<uint32_t>&   max_ints,
    const std::vector<uint64_t>&   sum_ints,
    uint32_t                       total_scans,
    const AnalysisTdfConfig&       cfg)
{
    namespace fs = std::filesystem;
    using namespace atdf_detail;

    // -----------------------------------------------------------------------
    // 1. Copy template
    // -----------------------------------------------------------------------
    auto out_tdf = output_dir / "analysis.tdf";
    std::error_code ec;
    fs::copy_file(tdf_src, out_tdf, fs::copy_options::overwrite_existing, ec);
    if (ec) {
        std::print(stderr, "write_analysis_tdf: copy {} -> {} failed: {}\n",
                   tdf_src.string(), out_tdf.string(), ec.message());
        return 1;
    }

    // -----------------------------------------------------------------------
    // 2. Open SQLite
    // -----------------------------------------------------------------------
    sqlite3* db_raw = nullptr;
    if (sqlite3_open(out_tdf.c_str(), &db_raw) != SQLITE_OK) {
        std::print(stderr, "write_analysis_tdf: cannot open {}: {}\n",
                   out_tdf.string(), sqlite3_errmsg(db_raw));
        sqlite3_close(db_raw);
        return 1;
    }
    DBGuard db_guard(db_raw);
    sqlite3* db = db_raw;

    // exec helper: run a SQL string and print any error
    auto exec = [&](const char* sql) -> bool {
        char* err = nullptr;
        int rc = sqlite3_exec(db, sql, nullptr, nullptr, &err);
        if (rc != SQLITE_OK) {
            std::print(stderr, "SQL error: {}\n  SQL: {}\n",
                       err ? err : sqlite3_errmsg(db), sql);
            sqlite3_free(err);
            return false;
        }
        return true;
    };

    if (!exec("PRAGMA journal_mode=WAL;")) return 1;
    if (!exec("BEGIN;"))                   return 1;

    // -----------------------------------------------------------------------
    // 3. Read defaults (T1, T2, Pressure) from template Frames table
    // -----------------------------------------------------------------------
    double T1 = 25.0, T2 = 25.0, pressure_val = 0.0;
    {
        sqlite3_stmt* s = nullptr;
        if (sqlite3_prepare_v2(db,
                "SELECT T1, T2, Pressure FROM Frames LIMIT 1",
                -1, &s, nullptr) == SQLITE_OK) {
            if (sqlite3_step(s) == SQLITE_ROW) {
                T1           = cfg.T1       ? *cfg.T1       : sqlite3_column_double(s, 0);
                T2           = cfg.T2       ? *cfg.T2       : sqlite3_column_double(s, 1);
                pressure_val = cfg.pressure ? *cfg.pressure
                             : (sqlite3_column_type(s, 2) != SQLITE_NULL
                                    ? sqlite3_column_double(s, 2) : 0.0);
            } else {
                if (cfg.T1)       T1           = *cfg.T1;
                if (cfg.T2)       T2           = *cfg.T2;
                if (cfg.pressure) pressure_val = *cfg.pressure;
            }
            sqlite3_finalize(s);
        }
    }

    // -----------------------------------------------------------------------
    // 4. Read representative FrameProperties from template
    // -----------------------------------------------------------------------
    std::vector<PropVal> ms1_props, ms2_props;
    {
        auto get_rep_frame_id = [&](int msms_type) -> int {
            sqlite3_stmt* s = nullptr;
            int fid = -1;
            const char* sql = (msms_type == 0)
                ? "SELECT Id FROM Frames WHERE MsMsType=0 LIMIT 1"
                : "SELECT Id FROM Frames WHERE MsMsType=9 LIMIT 1";
            if (sqlite3_prepare_v2(db, sql, -1, &s, nullptr) == SQLITE_OK) {
                if (sqlite3_step(s) == SQLITE_ROW) fid = sqlite3_column_int(s, 0);
                sqlite3_finalize(s);
            }
            return fid;
        };

        int ms1_rep = get_rep_frame_id(0);
        int ms2_rep = get_rep_frame_id(9);
        if (ms1_rep >= 0) ms1_props = read_frame_props(db, ms1_rep);
        if (ms2_rep >= 0) ms2_props = read_frame_props(db, ms2_rep);
    }

    // -----------------------------------------------------------------------
    // 5. DELETE dynamic table contents
    // -----------------------------------------------------------------------
    if (!exec("DELETE FROM Frames;") ||
        !exec("DELETE FROM DiaFrameMsMsInfo;") ||
        !exec("DELETE FROM Segments;") ||
        !exec("DELETE FROM FrameProperties;")) {
        free_props(ms1_props); free_props(ms2_props);
        return 1;
    }

    // -----------------------------------------------------------------------
    // 6. INSERT Frames rows
    // -----------------------------------------------------------------------
    {
        const char* sql =
            "INSERT INTO Frames"
            " (Id, Time, Polarity, ScanMode, MsMsType, TimsId, MaxIntensity,"
            "  SummedIntensities, NumScans, NumPeaks, MzCalibration, T1, T2,"
            "  TimsCalibration, PropertyGroup, AccumulationTime, RampTime,"
            "  Pressure, Denoised)"
            " VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";

        sqlite3_stmt* s = nullptr;
        if (sqlite3_prepare_v2(db, sql, -1, &s, nullptr) != SQLITE_OK) {
            std::print(stderr, "prepare INSERT Frames: {}\n", sqlite3_errmsg(db));
            free_props(ms1_props); free_props(ms2_props);
            return 1;
        }

        uint32_t first_fid = frame_index.empty() ? 1u : frame_index.front().frame_id;
        double   period_s  = (cfg.accumulation_time_ms + cfg.ramp_time_ms) / 1000.0;
        const char* pol_str = (cfg.polarity == '-') ? "-" : "+";

        for (size_t i = 0; i < frame_index.size(); ++i) {
            const auto& fe       = frame_index[i];
            int         msms_type = (fe.source == MS2) ? 9 : 0;
            double      time      = (double)(fe.frame_id - first_fid) * period_s;

            sqlite3_bind_int   (s,  1, (int)ids[i]);
            sqlite3_bind_double(s,  2, time);
            sqlite3_bind_text  (s,  3, pol_str, 1, SQLITE_STATIC);
            sqlite3_bind_int   (s,  4, cfg.scan_mode);
            sqlite3_bind_int   (s,  5, msms_type);
            sqlite3_bind_int64 (s,  6, (sqlite3_int64)tims_ids[i]);
            sqlite3_bind_int   (s,  7, (int)max_ints[i]);
            sqlite3_bind_int64 (s,  8, (sqlite3_int64)sum_ints[i]);
            sqlite3_bind_int   (s,  9, (int)total_scans);
            sqlite3_bind_int   (s, 10, (int)num_peaks[i]);
            sqlite3_bind_int   (s, 11, cfg.mz_calibration);
            sqlite3_bind_double(s, 12, T1);
            sqlite3_bind_double(s, 13, T2);
            sqlite3_bind_int   (s, 14, 1);   // TimsCalibration FK
            sqlite3_bind_int   (s, 15, 1);   // PropertyGroup FK
            sqlite3_bind_double(s, 16, cfg.accumulation_time_ms);
            sqlite3_bind_double(s, 17, cfg.ramp_time_ms);
            sqlite3_bind_double(s, 18, pressure_val);
            sqlite3_bind_int   (s, 19, cfg.denoised);

            if (sqlite3_step(s) != SQLITE_DONE) {
                std::print(stderr, "INSERT Frames failed (frame {}): {}\n",
                           fe.frame_id, sqlite3_errmsg(db));
                sqlite3_finalize(s);
                free_props(ms1_props); free_props(ms2_props);
                return 1;
            }
            sqlite3_reset(s);
            sqlite3_clear_bindings(s);
        }
        sqlite3_finalize(s);
    }

    // -----------------------------------------------------------------------
    // 7. INSERT FrameProperties rows
    //    For each frame, replicate the representative MS1 or MS2 property set.
    // -----------------------------------------------------------------------
    if (!ms1_props.empty() || !ms2_props.empty()) {
        sqlite3_stmt* s = nullptr;
        if (sqlite3_prepare_v2(db,
                "INSERT INTO FrameProperties (Frame, Property, Value) VALUES (?,?,?)",
                -1, &s, nullptr) != SQLITE_OK) {
            std::print(stderr, "prepare INSERT FrameProperties: {}\n", sqlite3_errmsg(db));
            free_props(ms1_props); free_props(ms2_props);
            return 1;
        }

        for (const auto& fe : frame_index) {
            const auto& props = (fe.source == MS2) ? ms2_props : ms1_props;
            for (const auto& [prop, val] : props) {
                sqlite3_bind_int  (s, 1, (int)fe.frame_id);
                sqlite3_bind_int  (s, 2, prop);
                sqlite3_bind_value(s, 3, val);
                if (sqlite3_step(s) != SQLITE_DONE) {
                    std::print(stderr, "INSERT FrameProperties failed: {}\n",
                               sqlite3_errmsg(db));
                    sqlite3_finalize(s);
                    free_props(ms1_props); free_props(ms2_props);
                    return 1;
                }
                sqlite3_reset(s);
                sqlite3_clear_bindings(s);
            }
        }
        sqlite3_finalize(s);
    }
    free_props(ms1_props);
    free_props(ms2_props);

    // -----------------------------------------------------------------------
    // 8. INSERT DiaFrameMsMsInfo rows (cycling WindowGroup 1..N_wg)
    // -----------------------------------------------------------------------
    {
        int n_wg = 0;
        {
            sqlite3_stmt* s = nullptr;
            if (sqlite3_prepare_v2(db,
                    "SELECT COUNT(*) FROM DiaFrameMsMsWindowGroups",
                    -1, &s, nullptr) == SQLITE_OK) {
                if (sqlite3_step(s) == SQLITE_ROW) n_wg = sqlite3_column_int(s, 0);
                sqlite3_finalize(s);
            }
        }

        if (n_wg > 0) {
            sqlite3_stmt* s = nullptr;
            if (sqlite3_prepare_v2(db,
                    "INSERT INTO DiaFrameMsMsInfo (Frame, WindowGroup) VALUES (?,?)",
                    -1, &s, nullptr) != SQLITE_OK) {
                std::print(stderr, "prepare INSERT DiaFrameMsMsInfo: {}\n",
                           sqlite3_errmsg(db));
                return 1;
            }

            int ms2_counter = 0;
            for (const auto& fe : frame_index) {
                if (fe.source == MS2) {
                    ++ms2_counter;
                    int wg = ((ms2_counter - 1) % n_wg) + 1;
                    sqlite3_bind_int(s, 1, (int)fe.frame_id);
                    sqlite3_bind_int(s, 2, wg);
                    if (sqlite3_step(s) != SQLITE_DONE) {
                        std::print(stderr, "INSERT DiaFrameMsMsInfo failed: {}\n",
                                   sqlite3_errmsg(db));
                        sqlite3_finalize(s);
                        return 1;
                    }
                    sqlite3_reset(s);
                    sqlite3_clear_bindings(s);
                }
            }
            sqlite3_finalize(s);
        }
    }

    // -----------------------------------------------------------------------
    // 9. INSERT Segments row
    // -----------------------------------------------------------------------
    if (!frame_index.empty()) {
        sqlite3_stmt* s = nullptr;
        if (sqlite3_prepare_v2(db,
                "INSERT INTO Segments (Id, FirstFrame, LastFrame, IsCalibrationSegment)"
                " VALUES (1,?,?,0)",
                -1, &s, nullptr) == SQLITE_OK) {
            sqlite3_bind_int(s, 1, (int)frame_index.front().frame_id);
            sqlite3_bind_int(s, 2, (int)frame_index.back().frame_id);
            if (sqlite3_step(s) != SQLITE_DONE)
                std::print(stderr, "INSERT Segments failed: {}\n", sqlite3_errmsg(db));
            sqlite3_finalize(s);
        }
    }

    // -----------------------------------------------------------------------
    // 10. Optionally override TimsCalibration C0..C9
    // -----------------------------------------------------------------------
    if (cfg.calib_params) {
        const auto& c = *cfg.calib_params;
        sqlite3_stmt* s = nullptr;
        if (sqlite3_prepare_v2(db,
                "UPDATE TimsCalibration"
                " SET C0=?,C1=?,C2=?,C3=?,C4=?,C5=?,C6=?,C7=?,C8=?,C9=?",
                -1, &s, nullptr) == SQLITE_OK) {
            for (int k = 0; k < 10; ++k)
                sqlite3_bind_double(s, k + 1, c[k]);
            sqlite3_step(s);
            sqlite3_finalize(s);
        }
    }

    // -----------------------------------------------------------------------
    // 11. Optionally replace DiaFrameMsMsWindows from mmappet
    // -----------------------------------------------------------------------
    if (cfg.dia_windows_path) {
        auto wg_col = OpenColumn<uint32_t>(*cfg.dia_windows_path, "WindowGroup");
        auto sb_col = OpenColumn<uint32_t>(*cfg.dia_windows_path, "ScanNumBegin");
        auto se_col = OpenColumn<uint32_t>(*cfg.dia_windows_path, "ScanNumEnd");
        auto mz_col = OpenColumn<double>  (*cfg.dia_windows_path, "IsolationMz");
        auto iw_col = OpenColumn<double>  (*cfg.dia_windows_path, "IsolationWidth");
        auto ce_col = OpenColumn<double>  (*cfg.dia_windows_path, "CollisionEnergy");

        if (!exec("DELETE FROM DiaFrameMsMsWindows;") ||
            !exec("DELETE FROM DiaFrameMsMsWindowGroups;"))
            return 1;

        // Insert unique WindowGroup IDs
        {
            std::set<uint32_t> unique_wg;
            for (size_t i = 0; i < wg_col.size(); ++i) unique_wg.insert(wg_col[i]);
            sqlite3_stmt* s = nullptr;
            if (sqlite3_prepare_v2(db,
                    "INSERT INTO DiaFrameMsMsWindowGroups (Id) VALUES (?)",
                    -1, &s, nullptr) == SQLITE_OK) {
                for (uint32_t wg : unique_wg) {
                    sqlite3_bind_int(s, 1, (int)wg);
                    sqlite3_step(s);
                    sqlite3_reset(s);
                    sqlite3_clear_bindings(s);
                }
                sqlite3_finalize(s);
            }
        }

        // Insert window rows
        {
            sqlite3_stmt* s = nullptr;
            if (sqlite3_prepare_v2(db,
                    "INSERT INTO DiaFrameMsMsWindows"
                    " (WindowGroup,ScanNumBegin,ScanNumEnd,IsolationMz,IsolationWidth,CollisionEnergy)"
                    " VALUES (?,?,?,?,?,?)",
                    -1, &s, nullptr) == SQLITE_OK) {
                for (size_t i = 0; i < wg_col.size(); ++i) {
                    sqlite3_bind_int   (s, 1, (int)wg_col[i]);
                    sqlite3_bind_int   (s, 2, (int)sb_col[i]);
                    sqlite3_bind_int   (s, 3, (int)se_col[i]);
                    sqlite3_bind_double(s, 4, mz_col[i]);
                    sqlite3_bind_double(s, 5, iw_col[i]);
                    sqlite3_bind_double(s, 6, ce_col[i]);
                    sqlite3_step(s);
                    sqlite3_reset(s);
                    sqlite3_clear_bindings(s);
                }
                sqlite3_finalize(s);
            }
        }
    }

    // -----------------------------------------------------------------------
    // 12. Commit
    // -----------------------------------------------------------------------
    if (!exec("COMMIT;")) {
        exec("ROLLBACK;");
        return 1;
    }

    std::print("wrote analysis.tdf ({} frames)\n", frame_index.size());
    return 0;
}
