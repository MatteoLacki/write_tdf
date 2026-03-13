"""Update analysis.tdf Frames table from frames_metadata.mmappet written by pmsms2tdf."""
import sys
import sqlite3
import numpy as np
from pathlib import Path

dot_d = Path(sys.argv[1])
meta = dot_d / "frames_metadata.mmappet"

ids  = np.fromfile(meta / "0.bin", dtype=np.uint32)
tids = np.fromfile(meta / "1.bin", dtype=np.uint64)
npks = np.fromfile(meta / "2.bin", dtype=np.uint32)
maxI = np.fromfile(meta / "3.bin", dtype=np.uint32)
sumI = np.fromfile(meta / "4.bin", dtype=np.uint64)

con = sqlite3.connect(dot_d / "analysis.tdf")
cur = con.cursor()
cur.executemany(
    "UPDATE Frames SET TimsId=?, NumPeaks=?, MaxIntensity=?, SummedIntensities=? WHERE Id=?",
    zip(tids.tolist(), npks.tolist(), maxI.tolist(), sumI.tolist(), ids.tolist()),
)
con.commit()
con.close()
print(f"Updated {len(ids)} frames in {dot_d / 'analysis.tdf'}")
