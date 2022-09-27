```
subtool <COMMAND> [opts] [FILE]

Available commands: info, show, repoint, dt, dump

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
  --format=FMT            Output format (default: pretty)
                            pretty   Table aligned values with headings.
                            csv      Comma-separated values.
                            bin      Raw binary data.

REPOINT COMMAND (repoint)
Apply a delay table to a subfile, or undo existing delays, creating a new
subfile as output.

      subtool repoint [repoint_opts] <INPUT_FILE> <OUTPUT_FILE>

  INPUT_FILE              Path to input subfile.
  OUTPUT_FILE             Path to write output subfile.
  --apply=PATH            Load the delay table to be applied from a file.
  --zero                  Remove all existing delays.
  --force                 Apply delays even if it would cause data loss.

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
  ```