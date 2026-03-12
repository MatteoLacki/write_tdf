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

=== Table: CalibrationInfo (27 rows total) ===
KeyPolarity                          KeyName                                                                                                                                                                                                                                                                                                                                         Value
          +              CalibrationDateTime                                                                                                                                                                                                                                                                                                                     2023-10-04T12:30:21+02:00
          +                  CalibrationUser                                                                                                                                                                                                                                                                                                                                       unknown
          +              CalibrationSoftware                                                                                                                                                                                                                                                                                                                                       timsTOF
          +       CalibrationSoftwareVersion                                                                                                                                                                                                                                                                                                                                         5.0.4
          +                MzCalibrationMode                                                                                                                                                                                                                                                                                                                                             4
          +           MzStandardDeviationPPM                                                                                                                                                                                                                                                                                                                                      0.891239
          +                ReferenceMassList                                                                                                                                                                                                                                                                                                                                    Na Formate
          + MzCalibrationSpectrumDescription                                                                                                                                                                                                                                                                                                                                     <unknown>
          +           ReferenceMassPeakNames                                                                                                b'Na(NaCOOH)4\x00Na(NaCOOH)5\x00Na(NaCOOH)6\x00Na(NaCOOH)7\x00Na(NaCOOH)8\x00Na(NaCOOH)10\x00Na(NaCOOH)11\x00Na(NaCOOH)12\x00Na(NaCOOH)13\x00Na(NaCOOH)15\x00Na(NaCOOH)17\x00Na(NaCOOH)18\x00Na(NaCOOH)19\x00Na(NaCOOH)21\x00Na(NaCOOH)22\x00'
          +              ReferencePeakMasses b"\x95+\xbc\xcb\x05or@\xacp\xcbG\xd2\xaev@>%\xe7\xc4\x9e\xeez@\xcf\xd9\x02Bk.\x7f@s\x0f\t\xdf\x1b\xb7\x81@H\x8c\x9e[\xe8\xf6\x85@\x90f,\x9a\xce\x16\x88@\x1c\t4\xd8\xb46\x8a@e\xe3\xc1\x16\x9bV\x8c@\x1d\xb0\xab\xc93K\x90@\x87n\xf6\x07\x1ak\x92@\xab[='\r{\x93@\xf1,AF\x00\x8b\x94@[\xeb\x8b\x84\xe6\xaa\x96@\x80\xd8\xd2\xa3\xd9\xba\x97@"

=== Table: CollisionEnergySweepingInfo (0 rows total) ===
Empty DataFrame
Columns: [Frame, CollisionId, CollisionEnergy, CollisionEnergyPercent]
Index: []

=== Table: DiaFrameMsMsInfo (7967 rows total) ===
 Frame  WindowGroup
     2            1
     3            2
     4            3
     5            4
     6            5
     8            6
     9            7
    10            8
    11            9
    12           10

=== Table: DiaFrameMsMsWindowGroups (10 rows total) ===
 Id
  1
  2
  3
  4
  5
  6
  7
  8
  9
 10

=== Table: DiaFrameMsMsWindows (4640 rows total) ===
 WindowGroup  ScanNumBegin  ScanNumEnd  IsolationMz  IsolationWidth  CollisionEnergy
           1             0           1  1124.550226            24.0        53.066298
           1             1           2  1122.729080            24.0        53.004144
           1             2           3  1120.907934            24.0        52.941989
           1             3           4  1119.086788            24.0        52.879834
           1             4           5  1117.265641            24.0        52.817680
           1             5           6  1115.444495            24.0        52.755525
           1             6           7  1113.623349            24.0        52.693370
           1             7           8  1111.802203            24.0        52.631215
           1             8           9  1109.981057            24.0        52.569061
           1             9          10  1108.159911            24.0        52.506906

=== Table: ErrorLog (0 rows total) ===
Empty DataFrame
Columns: [Frame, Scan, Message]
Index: []

=== Table: FrameMsMsInfo (0 rows total) ===
Empty DataFrame
Columns: [Frame, Parent, TriggerMass, IsolationWidth, PrecursorCharge, CollisionEnergy]
Index: []

=== Table: FrameProperties (1507447 rows total) ===
 Frame  Property                                                                                                                   Value
     1         9                                                                                                                       1
     1        18 b'\x00\x00\x00\x00\x00\x00\xf0?\x00\x00\x00\x00\x00\x00\xf0?\x00\x00\x00\x00\x00\x00\xf0?\x00\x00\x00\x00\x00\x00\xf0?'
     1        19                                                                                                                     464
     1        41                                                                                                             2352.936509
     1       214                                                                                                                   108.0
     1       249                                                                                                                     0.0
     1       280                                                                                                                       1
     1       281                                                                                                                       1
     1       282                                                                                                                    15.0
     1       283                                                                                                                       0

=== Table: Frames (9561 rows total) ===
 Id     Time Polarity  ScanMode  MsMsType  TimsId  MaxIntensity  SummedIntensities  NumScans  NumPeaks  MzCalibration        T1        T2  TimsCalibration  PropertyGroup  AccumulationTime  RampTime  Pressure  Denoised
  1 0.481476        +         9         0      64         29132           43281218       464    281314              1 25.694761 25.477029                1              1            50.004    50.004  2.657222         0
  2 0.538425        +         9         9  662182         35997            3379272       464     12555              1 25.694761 25.477029                1              1            50.004    50.004  2.657222         0
  3 0.594423        +         9         9  702383          9457            1968840       464      8880              1 25.694761 25.477029                1              1            50.004    50.004  2.657222         0
  4 0.650812        +         9         9  732112           798            1264959       464      6046              1 25.694761 25.477029                1              1            50.004    50.004  2.657222         0
  5 0.706996        +         9         9  753169          3862             994786       464      4604              1 25.694761 25.477029                1              1            50.004    50.004  2.657222         0
  6 0.762891        +         9         9  769607          1678             779370       464      3699              1 25.694761 25.477029                1              1            50.004    50.004  2.657222         0
  7 0.820695        +         9         0  783010         26088           40038313       464    260172              1 25.694761 25.477029                1              1            50.004    50.004  2.657222         0
  8 0.875700        +         9         9 1397207         32341            2483427       464      9803              1 25.694761 25.477029                1              1            50.004    50.004  2.657222         0
  9 0.932504        +         9         9 1429716           710            1482307       464      7057              1 25.694761 25.477029                1              1            50.004    50.004  2.657222         0
 10 0.989512        +         9         9 1453941          2202            1071054       464      5147              1 25.694761 25.477029                1              1            50.004    50.004  2.657222         0

=== Table: GlobalMetadata (35 rows total) ===
                      Key                                Value
               SchemaType                                  TDF
       SchemaVersionMajor                                    3
       SchemaVersionMinor                                    8
AcquisitionSoftwareVendor                               Bruker
         InstrumentVendor                               Bruker
           ClosedProperly                                    1
      TimsCompressionType                                    2
       MaxNumPeaksPerScan                                 2219
               AnalysisId 61925e66-96f3-453b-893c-0bc957d6b178
      DigitizerNumSamples                               395286

=== Table: GroupProperties (282 rows total) ===
 PropertyGroup  Property  Value
             1         0    0.0
             1         1   25.5
             1         2 1500.0
             1         3   70.0
             1         4   25.5
             1         5   10.0
             1         6    6.0
             1         7   10.0
             1         8   36.0
             1         9    0.0

=== Table: MzCalibration (1 rows total) ===
 Id  ModelType  DigitizerTimebase  DigitizerDelay       T1        T2  dC1  dC2         C0            C1       C2  C3        C4
  1          1                0.2         25632.6 25.64309 25.041002 20.0  0.0 326.530846 156091.442531 0.002319 0.0 -0.040173

=== Table: PrmFrameMeasurementMode (0 rows total) ===
Empty DataFrame
Columns: [Frame, MeasurementModeId]
Index: []

=== Table: PrmFrameMsMsInfo (0 rows total) ===
Empty DataFrame
Columns: [Frame, ScanNumBegin, ScanNumEnd, IsolationMz, IsolationWidth, CollisionEnergy, Target]
Index: []

=== Table: PrmTargets (0 rows total) ===
Empty DataFrame
Columns: [Id, ExternalId, Time, OneOverK0, MonoisotopicMz, Charge, Description]
Index: []

=== Table: PropertyDefinitions (456 rows total) ===
 Id                 PermanentName  Type DisplayGroupName                          DisplayName DisplayValueText DisplayFormat DisplayDimension Description
  0       Calibration_MarkSegment     0      Calibration         Mark TOF Calibration Segment       0:No;1:Yes            %d                             
  1            Collision_Bias_Set     1   Collision Cell              Set Collision Cell Bias                           %.1f                V            
  2              Collision_RF_Set     1   Collision Cell                Set Collision Cell RF                           %.1f              Vpp            
  3              Collision_In_Set     1   Collision Cell                Set Collision Cell In                           %.1f                V            
  4             Collision_Out_Set     1   Collision Cell               Set Collision Cell Out                           %.1f                V            
  5   Collision_Energy_Offset_Set     1   Collision Cell          Set Collision Energy Offset                           %.1f               eV            
  6      Collision_QuenchTime_Set     0   Collision Cell                       Set QuenchTime                             %d               ms            
  7  Collision_ATS_QuenchTime_Set     0   Collision Cell Set QuenchTime (IMS Stepping active)                             %d               ms            
  8       Collision_GasSupply_Set     1   Collision Cell    Set Collision Gas Supply Flowrate                           %.1f                %            
  9 Collision_GasSupplySwitch_Set     0   Collision Cell      Set Collision Gas Supply Switch       0:Off;1:On            %d                             

=== Table: PropertyGroups (1 rows total) ===
 Id
  1

=== Table: Segments (1 rows total) ===
 Id  FirstFrame  LastFrame  IsCalibrationSegment
  1           1       9561                     0

=== Table: TimsCalibration (1 rows total) ===
 Id  ModelType  C0  C1         C2        C3        C4  C5      C6         C7        C8          C9
  1          2   1 463 177.529164 86.022472 33.333333   1 0.05016 127.655959 12.634162 3765.192817

