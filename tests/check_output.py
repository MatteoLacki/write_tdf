"""Verify pmsms2tdf output against the input mmappet."""
import sys
import numpy as np

input_mmappet = sys.argv[1]   # e.g. ../../temp/test/deduplicated_precursors.mmappet
output_dot_d  = sys.argv[2]   # e.g. /tmp/test_output.d

meta = output_dot_d + "/frames_metadata.mmappet"

frames_in = np.fromfile(f"{input_mmappet}/0.bin", dtype=np.uint32)
ints_in   = np.fromfile(f"{input_mmappet}/3.bin", dtype=np.uint32)

ids  = np.fromfile(f"{meta}/0.bin", dtype=np.uint32)
tids = np.fromfile(f"{meta}/1.bin", dtype=np.uint64)
npks = np.fromfile(f"{meta}/2.bin", dtype=np.uint32)
maxI = np.fromfile(f"{meta}/3.bin", dtype=np.uint32)
sumI = np.fromfile(f"{meta}/4.bin", dtype=np.uint64)

print(f"{'Frame':>6}  {'TimsId':>8}  {'NumPeaks':>8}  {'MaxInt':>8}  {'SumInt':>12}  {'Check':>5}")
print("-" * 60)

errors = 0
for i, fid in enumerate(ids):
    mask = frames_in == fid
    ok_npks = npks[i] == mask.sum()
    ok_maxI = maxI[i] == ints_in[mask].max()
    ok_sumI = sumI[i] == int(ints_in[mask].sum())
    status = "OK" if (ok_npks and ok_maxI and ok_sumI) else "FAIL"
    if status == "FAIL":
        errors += 1
    print(f"{fid:>6}  {tids[i]:>8}  {npks[i]:>8}  {maxI[i]:>8}  {sumI[i]:>12}  {status:>5}")

print()
if errors:
    print(f"FAILED: {errors} frame(s) had mismatches")
    sys.exit(1)
else:
    print(f"All {len(ids)} frames OK")
