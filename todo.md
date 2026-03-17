* Add sqlite.h if possible and check if linking against it completes the roundtrip.

* Write a command line programme pmsms2tdf.

Expected inputs:

pmsms2tdf deduplicated_precursors.mmappet path_to_analysis_tdf_bin.


* it takes as input a .mmappet folder with columns:

df_head temp/test/deduplicated_precursors.mmappet
     frame  scan    tof  intensity
0       81   298  49998          1
1       81   298  49999          2
..     ...   ...    ...        ...
465    371   252  45000          5
466    371   252  45001          2

[467 rows x 4 columns]

that have types: 
main (common) matteo@pingu$ cat temp/test/deduplicated_precursors.mmappet/schema.txt 
uint32 frame
uint32 scan
uint32 tof
uint32 intensity

    * Those types will stay as above for sure.
    * The dataset can have other columns, so use OpenColumn from mmappet.h to control which columns get open.

and write it into the <outuput>.d/analysis.tdf_bin file, where `output` is user specified and read from the command line, together with the path to the .mmappet data.

    * Use zlib as it was system wide available, see Makefile import.

The programme should follow the example of how to translate code provided in git/savetimspy/savetimspy/writer.py in SaveTIMS class, method save_frame_tofs to be precise.
    * scans, tofs, intensities would be read from the .mmappet file, in groups defined by the frame columm.
    * therefore we need frame column to be indexed with a cumulated sum index, telling where each new group starts.
    * the data can be assumed to be sorted and already deduplicated.
    each frame should give rise to another Id right now, staring from 1 and getting higher (check examplary Frames table in example git/write_tdf/tests/F9468.d/analysis.tdf)  

For now, we do not want but to create an analysis.tdf_bin file, but as I don't want to work with sqlite from C++ but from python later on, I will need necessary outputs from the C++ code. 

* In .mmappet folder
* A dataframe with columns:
    * Id, TimsId, NumPeaks, MaxIntensity, as defined per row in writer.py in lines:
        updated_analysis_tdf_row["TimsId"] = frame_start_pos
        updated_analysis_tdf_row["NumPeaks"] = num_peaks
        updated_analysis_tdf_row["MaxIntensity"] = (
            0 if len(intensities) == 0 else int(np.max(intensities))
        )
        updated_analysis_tdf_row["SummedIntensities"] = int(np.sum(intensities))

* The example input .mmappet data is in `temp/test/deduplicated_precursors.mmappet`.
    * if absent, run `./smk -call temp/test/deduplicated_precursors.mmappet` to reconstruct it (snakemake pipeline)



 OK, now look at contents of /home/matteo/Projects/simulators/massimo_pipeline/massimo_pipeline/git/write_tdf/tests/F9477 there are two mmappet folders there that         
  contain ms1 and ms2 data. you can use the df_head tool from pandas_ops instgalled in venvs/common/bin to check the inputs and you can inspect the types in schema files.  
  This files are created from an actual dataset that I put for reference in tests/F9477.d. I would now like to get modify the current tool so that                          
────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────


* To check the round trip, I have made data for you in :

df/tests/F9477_ms{1,2}.mmappet
/home/matteo/Projects/simulators/massimo_pipeline/massimo_pipeline/git/write_tdf/tests/F9477_ms1.mmappet
           frame  scan     tof  intensity
0              1    33  228285        178
1              1    33  237451        178
...          ...   ...     ...        ...
177537967   9560   463  300306         98
177537968   9560   463  332184        170

[177537969 rows x 4 columns]

/home/matteo/Projects/simulators/massimo_pipeline/massimo_pipeline/git/write_tdf/tests/F9477_ms2.mmappet
           frame  scan     tof  intensity
0              2    33  139746        260
1              2    33  237605        108
...          ...   ...     ...        ...
160689738   9559   433  383108         20
160689739   9559   435  252854         20

[160689740 rows x 4 columns]

(common) 

