const USAGE = `subtool <COMMAND> [opts] [FILE]

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
  --source=ID             Extract all the samples from a given source ID.


`

const schema = {
  show: {
    args: ['FILE'],
    opts: {
      "--header": {
        type: "flag",
        prop: "show_header",
      },
      "--data": {
        type: "flag",
        prop: "show_data",
      },
      "--delay-table": {
        type: "flag",
        prop: "show_delay_table",
      },
      "--source": {
        type: "uint-list",
        prop: "selected_sources",
      },
      "--samples": {
        type: "uint",
        prop: "num_samples",
      },
      "--frac-delays": {
        type: "uint",
        prop: "num_frac_delays",
      },
      "--format-out": {
        type: "enum",
        values: ["pretty", "csv", "bin"],
        prop: "format_out",
      },
    },
    defaults: {
      show_header: false,
      show_delay_table: false,
      show_data: false,
      selected_sources: null,
      num_samples: null,
      num_frac_delays: null,
      format_out: "pretty",
    }
  },
  dt: {
    args: ["FILE"],
    opts: {
      "--format-in": {
        type: "enum",
        values: ["auto", "csv", "bin"],
        prop: "format_in",
      },
      "--format-out": {
        type: "enum",
        values: ["pretty", "csv", "bin"],
        prop: "format_out",
      },
      "--source": {
        type: "uint-list",
        prop: "selected_sources",
      },
      "--frac-delays": {
        type: "uint",
        prop: "num_frac_delays",
      },
      "--num-sources": {
        type: "uint",
        prop: "num_frac_delays",
      },
      "--compare": {
        type: "string",
        prop: "compare_file",
      },
    },
    defaults: {
      format_in: "auto",
      format_out: "pretty",
      selected_sources: null,
      num_frac_delays: null,
      num_sources: null,
      compare_file: null,
    },
  },
  repoint: {
    args: ["INPUT_FILE", "OUTPUT_FILE"],
    opts: {
      "--apply": {
        type: "string",
        prop: "delay_table_filename",
      },
      "--zero": {
        type: "flag",
        prop: "zero_delays",
      },
      "--force": {
        type: "flag",
        prop: "force_delays",
      },
    },
    defaults: {
      delay_table_filename: null,
      zero_delays: false,
      force_delays: false,
    },
  },
  info: {
    args: ["FILE"],
    opts: {
      "--hex": {
        type: "flag",
        prop: "hex_offsets",
      },
    },
    defaults: {
      hex_offsets: false
    },
  },
  dump: {
    args: ["SUBFILE", "OUTPUT_FILE"],
    opts: {
      "--section": {
        type: "enum",
        prop: "dump_section",
        values: ["header", "dt", "udpmap", "margin", "data", "preamble"],
      },
      "--block": {
        type: "uint",
        prop: "dump_block",
      },
    },
    defaults: {
      dump_section: null,
      dump_block: null,
    },
  }
}

function parseVal(str, shape) {
  let val = null
  if(str.length == 0) {
    return {status: 'invalid'}
  }
  switch(shape.type) {
  case 'uint':
    if(str.indexOf('.') != -1)
      return {status: 'invalid'}
    val = Number.parseInt(str)
    if(isNaN(val) || val < 0)
      return {status: 'invalid'}
    break
  case 'uint-list':
    val = []
    for(let x of str.split(',').map(x => parseVal(x, {type: 'uint'})))  {
      if(x.status == 'ok')
        val.push(x.val)
      else
        return {status: 'invalid'}
    }
    break
  case 'enum':
    if(shape.values.indexOf(str) == -1)
      return {status: 'invalid'}
    val = str
    break
  case 'string':
    val = str
    break
  default:
      throw `Invalid argument type ${shape.type}`
  }
  return {status: 'ok', val}
}

function parseOpt(name, remaining, optSchema, opts) {
  if(name.indexOf('=') != -1) {
    const [optName, optArg] = name.split('=')
    name = optName
    remaining = [optArg, ...remaining]
  }

  if(!(name in optSchema))
    return {status: 'err', reason: `${name} is not a valid option.`}

  const shape = optSchema[name]
  if(shape.type == 'flag') {
    opts[shape.prop] = true
    return {status: 'ok', remaining}
  }

  if(remaining.length == 0)
    return {status: 'err', reason: `${name} requires an argument.`}

  const parseResult = parseVal(remaining[0], shape)
  if(parseResult.status == 'ok')
    opts[shape.prop] = parseResult.val
  else
    return {status: 'err', reason: `'${remaining[0]}' is not a valid argument for ${name}`}
  
  return {status: 'ok', remaining: remaining.slice(1)}
}

function parseOptions(args, optSchema, opts) {
  if(args.length == 0) 
    return {status: 'ok', opts}
  
  const [arg, ...remaining] = args

  if(!(arg[0] == '-' && arg.length > 1))
    return {status: 'err', reason: `Unexpected argument ${arg}`}

  const parseResult = parseOpt(arg, remaining, optSchema, opts)
  if(parseResult.status != 'ok')
    return parseResult

  return parseOptions(parseResult.remaining, optSchema, opts)
}

export function parseCommand(command, args) {
  if(!(command in schema))
    return {status: 'err', reason: `'${command}' is not a valid command. Run without arguments for usage information.`}

  const cmdSchema = schema[command]
  const opts = { ...cmdSchema.defaults }
  const numFixedArgs = cmdSchema.args.length
  const fixedArgs = args.slice(-numFixedArgs)
  if(fixedArgs.length < numFixedArgs)
    return {status: 'err', reason: `Missing argument ${cmdSchema.args[fixedArgs.length]}`}
  const numOptArgs = args.length - numFixedArgs
  const optArgs = args.slice(0, numOptArgs)
  const parseResult = parseOptions(optArgs, cmdSchema.opts, opts)
  if(parseResult.status != 'ok')
    return parseResult
  return {status: 'ok', command, fixedArgs, opts}
}

export function parseCommandLine(argList) {
  if(argList.length == 0) {
    console.log(USAGE)
    return {status: 'ok', command: null}
  }

  const [command, ...args] = argList
  return parseCommand(command, args)
}