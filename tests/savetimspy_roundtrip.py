"""Minimal roundtrip test using savetimspy core logic (no savetimspy import)."""
import shutil, sqlite3, struct, numpy as np
from pathlib import Path
from opentimspy import OpenTIMS
import zstd


def get_peak_cnts(total_scans, scans):
    peak_cnts = [total_scans]
    ii = 0
    for scan_id in range(1, total_scans):
        counter = 0
        while ii < len(scans) and scans[ii] < scan_id:
            ii += 1
            counter += 1
        peak_cnts.append(counter * 2)
    return np.array(peak_cnts, np.uint32)


def modify_tofs(tofs, scans):
    last_tof = np.int64(-1)
    last_scan = int(scans[0]) - 1
    for ii in range(len(tofs)):
        if last_scan != scans[ii]:
            last_tof = -1
            last_scan = scans[ii]
        val = int(tofs[ii])
        tofs[ii] = np.uint32(val - last_tof)
        last_tof = val


def np_zip(xx, yy):
    res = np.empty(2 * len(xx), dtype=np.uint32)
    res[0::2] = xx
    res[1::2] = yy
    return res


def get_realdata(peak_cnts, interleaved):
    back_data = bytearray(peak_cnts.tobytes() + interleaved.tobytes())
    real_data = bytearray(len(back_data))
    reminder = 0
    bd_idx = 0
    for rd_idx in range(len(back_data)):
        if bd_idx >= len(back_data):
            reminder += 1
            bd_idx = reminder
        real_data[rd_idx] = back_data[bd_idx]
        bd_idx += 4
    return real_data


def write_frame(tdf_bin, scans, tofs, intensities, total_scans):
    frame_start = tdf_bin.tell()
    tofs = np.copy(tofs)
    scans = np.copy(scans)
    peak_cnts = get_peak_cnts(total_scans, scans)
    modify_tofs(tofs, scans)
    interleaved = np_zip(tofs, intensities)
    real_data = get_realdata(peak_cnts, interleaved)
    compressed = zstd.ZSTD_compress(bytes(real_data), 1)
    tdf_bin.write(struct.pack('<I', len(compressed) + 8))
    tdf_bin.write(struct.pack('<I', total_scans))
    tdf_bin.write(compressed)
    return frame_start, len(scans)


src_path = Path("tests/F9477.d")
dst_path = Path("/tmp/savetimspy_roundtrip.d")
shutil.rmtree(dst_path, ignore_errors=True)
dst_path.mkdir()

src = OpenTIMS(src_path)
total_scans = 464

shutil.copy(src_path / "analysis.tdf", dst_path / "analysis.tdf")

frame_meta = []  # (frame_id, tims_id, num_peaks)
with open(dst_path / "analysis.tdf_bin", "wb") as tdf_bin:
    for fid in [1, 2]:
        q = src.query(fid, columns=["scan", "tof", "intensity"])
        tims_id, npks = write_frame(
            tdf_bin,
            q["scan"].astype(np.uint32),
            q["tof"].astype(np.uint32),
            q["intensity"].astype(np.uint32),
            total_scans,
        )
        frame_meta.append((fid, tims_id, npks))

con = sqlite3.connect(dst_path / "analysis.tdf")
for fid, tims_id, npks in frame_meta:
    con.execute("UPDATE Frames SET TimsId=?, NumPeaks=?, AccumulationTime=100.0 WHERE Id=?", (tims_id, npks, fid))
con.commit(); con.close()

reco = OpenTIMS(dst_path)
print("Checking roundtrip...")
for fid, label in [(1, "MS1"), (2, "MS2")]:
    o = src[fid]; r = reco[fid]
    print(f"  Frame {fid} ({label}): orig[:5]={o[:5,3]}, reco[:5]={r[:5,3]}, match={np.array_equal(o[:,3], r[:,3])}")
