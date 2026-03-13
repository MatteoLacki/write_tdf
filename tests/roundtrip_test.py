"""Round-trip verification: original .d vs reconstructed .d via OpenTIMS.

Each frame array has columns: [frame, scan, tof, intensity] at indices 0-3.
"""
import sys
import numpy as np
from opentimspy import OpenTIMS

orig = OpenTIMS(sys.argv[1])
reco = OpenTIMS(sys.argv[2])

# Column indices in the returned 2D uint32 array
COL = {"scan": 1, "tof": 2, "intensity": 3}

frames = sorted(orig.ms1_frames.tolist() + orig.ms2_frames.tolist())
errors = 0
for fid in frames:
    o = orig[fid]
    r = reco[fid]
    for col, idx in COL.items():
        if not np.array_equal(o[:, idx], r[:, idx]):
            print(f"Frame {fid}: {col} mismatch (orig {len(o)} vs reco {len(r)} peaks)")
            errors += 1
            break

print()
if errors:
    print(f"FAILED: {errors}/{len(frames)} frames differ")
    sys.exit(1)
else:
    print(f"All {len(frames)} frames match.")
