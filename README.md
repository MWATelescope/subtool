# subtool - Swiss army knife for MWA subobservation files

## SYNOPSIS

```
subtool <COMMAND> [opts] [FILE]

Available commands:
  info, get, set, unset, show, dt, dump, bake
  repoint, replace, resample, patch, upgrade

INFO COMMAND (info)
Print a summary of information about the subfile.

      subtool info [info_opts] <FILE>

  FILE                    Path to input subfile.
  --hex                   Show byte offsets in hexadecimal.

SHOW COMMAND (show)
Extract selected information from a subfile and display in human-readable
format, or export as CSV or binary.

      subtool show [show_opts] <FILE>

  FILE                    Path to input subfile.
  --header                Print the 4K header fields.
  --delay-table           Print delay table.
  --data                  Print voltage data.
  --frac-delays=N         Output only the first N fractional delays.
  --samples=N             Output only the first N samples per source.
  --source=A[,B...]       Output only the specified RF sources.
  --block=N               Output voltage data from block N (default: 1).
  --format=FMT            Output format (default: pretty)
                            pretty   Table aligned values with headings.
                            csv      Comma-separated values.
                            bin      Raw binary data.

HEADER FIELD COMMANDS (get, set, unset)
Get and set values for header fields, or delete them.

      subtool get <KEY> <FILE>
      subtool set [hdr_opts] <KEY> <VALUE> <FILE>
      subtool unset [hdr_opts] <KEY> <FILE>

  KEY                     Name of header field.
  VALUE                   Value for header field.
  FILE                    Path to input subfile.
  --force                 Proceed even if key doesn't exist.

REPOINT COMMAND (repoint)
Apply a delay table to a subfile, or undo existing delays, creating a new
subfile as output.

      subtool repoint [repoint_opts] <INPUT_FILE> <OUTPUT_FILE>

  INPUT_FILE              Path to input subfile.
  OUTPUT_FILE             Path to write output subfile.
  --apply=PATH            Load the delay table to be applied from a file.
  --zero                  Remove all existing delays.
  --force                 Apply delays even if it would cause data loss.

REPLACE DATA COMMAND (replace)
Write a new subfile, replacing voltage data from specified sources with data
from other sources.

      subtool replace [replace_opts] <INPUT_FILE> <OUTPUT_FILE>

  INPUT_FILE              Path to input subfile.
  OUTPUT_FILE             Path to write output subfile.
  --map=A:B[,C:D...]      Voltages for source A taken from B's data.
  --map-all=A             Voltages for all sources are taken from A's data.

DELAY TABLE COMMAND (dt)
Read and write delay table files, select subsets and compare between them.

      subtool dt [dt_opts] <FILE>

  FILE                    Path to input delay table file.
  --frac-delays=N         Output only the first N fractional delays.
  --source=A[,B...]       Output only the specified RF sources.
  --num-sources-in=N      Number of sources in input (default: auto).
  --num-frac-delays-in=N  Number of sources in input (default: auto).
  --compare=REF           Show delay difference from file REF to FILE.
  --format-in=FMT         Input format (default: auto)
                            csv      Comma-separated values.
                            bin      Raw binary data.
                            auto     Auto-detect.
  --format-out=FMT        Output format (default: pretty)
                            pretty   Table aligned values with headings.
                            csv      Comma-separated values.
                            bin      Raw binary data.

DUMP COMMAND (dump)
Write binary contents of a subfile section to a file.

      subtool dump [dump_opts] <SUBFILE> <OUTPUT_FILE>

  SUBFILE                 Path to input subfile.
  OUTPUT_FILE             Path to output file.
  --section=SECTION       Section to extract:
                            header   Subfile header.
                            dt       Delay table.
                            udpmap   UDP packet map.
                            margin   Margin data.
                            data     Entire sample data section.
                            preamble Header + block 0.
  --block=N               Extract the Nth block (sample data starts at N=1).
  --source=ID             Extract all the samples from a given source ID.
  --with-margin           When extracting samples, include margin data.

RESAMPLE COMMAND (replace)
Write a new subfile, resampling voltage data from specified sources with
varying phase delays.

      subtool resample [resample_opts] <INPUT_FILE> <OUTPUT_FILE>

  INPUT_FILE              Path to input subfile.
  OUTPUT_FILE             Path to write output subfile.
  --rules=A[,B...]        Transform rule specifier list.
  --region=N              Transform with N surrounding samples.

BAKE-IN DELAYS COMMAND (bake)
Apply fractional delays to the voltage data in a subfile and reset the delay
table values to zero. Operates in-place.

      subtool bake <FILE>

  FILE                    Path to subfile.    
  --source=A[,B...]       Process only the specified RF sources.
  --fft-size=SIZE         Number of points in FFT (default: 8192).

PATCH COMMAND (patch)
Overwrite a section of a subfile with data loaded from a file.

      subtool patch [patch_opts] <PATCH> <SUBFILE>

  PATCH                   Path to replacement data.
  SUBFILE                 Path to subfile.
  --section=SECTION       Section to replace:
                            header   Subfile header.
                            dt       Delay table.
                            udpmap   UDP packet map.
                            margin   Margin data.
                            data     Entire sample data section.
                            preamble Header + block 0.

UPGRADE COMMAND (upgrade)
Upgrade a subfile to use microsample fractional delays.

      subtool upgrade <FILE>
  
  FILE                    Path to subfile.
```

## INSTALL

subtool is developed with NodeJS v18.10.0. Newer versions are expected to work,
older versions may also work.

```
$  git clone https://github.com/shmookey/subtool.git
$  cd subtool
$  npm install
$  npm run build
$  ./subtool
```

