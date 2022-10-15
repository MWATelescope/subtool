import {ok, fail, all} from "./util.js"
import {TransformSpec, Result} from "./types"

const USAGE = `subtool <COMMAND> [opts] [FILE]

Available commands:
  info, get, set, unset, show, dt, dump, repoint, replace, resample, bake

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
`
/*
SIGNAL PROCESSOR COMMAND (dsp)
Manipulate exported voltage data files.

      subtool dsp [dsp_opts] <INPUT_FILE> <OUTPUT_FILE>
  
  INPUT_FILE              Path to input subfile.
  OUTPUT_FILE             Path to write output subfile.
  --rule=<TRANSFORM>      Transform rule specifier.
`
*/

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
      "--block": {
        type: "uint",
        prop: "show_block",
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
      num_samples: 10,
      num_frac_delays: 10,
      format_out: "pretty",
      show_block: 1,
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
  get: {
    args: ["KEY", "FILE"],
    opts: {
    },
    defaults: {
    },
  },
  set: {
    args: ["KEY", "VALUE", "FILE"],
    opts: {
      "--force": {
        type: "flag",
        prop: "set_force",
      },
    },
    defaults: {
      set_force: false,
    },
  },
  unset: {
    args: ["KEY", "FILE"],
    opts: {
      "--force": {
        type: "flag",
        prop: "unset_force",
      },
    },
    defaults: {
      unset_force: false,
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
        prop: "repoint_zero",
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
  resample: {
    args: ["INPUT_FILE", "OUTPUT_FILE"],
    opts: {
      "--rules": {
        type: "transform-spec-list",
        prop: "resample_rules",
      },
      "--region": {
        type: "uint",
        prop: "resample_region",
      },
    },
    defaults: {
      resample_rules: [],
      resample_region: 3,
    },
  },
  replace: {
    args: ["INPUT_FILE", "OUTPUT_FILE"],
    opts: {
      "--map": {
        type: "mapping-list",
        prop: "replace_map",
      },
      "--map-all": {
        type: "uint",
        prop: "replace_map_all",
      },
    },
    defaults: {
      replace_map: null,
      replace_map_all: null,
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
      "--source": {
        type: "uint",
        prop: "dump_source",
      },
      "--with-margin": {
        type: "flag",
        prop: "dump_with_margin",
      }
    },
    defaults: {
      dump_section: null,
      dump_block: null,
      dump_source: null,
      dump_with_margin: true,
    },
  },
  bake: {
    args: ["SUBFILE"],
  },
  /*dsp: {
    args: ["INPUT_FILE", "OUTPUT_FILE"],
    opts: {
      "--rule": {
        type: "transform-spec",
        prop: "dsp_rule",
      },
    },
    defaults: {
      dsp_rule: [],
    },
  },*/
}

function parse_uint(str: string): Result<number> {
  const num = Number.parseInt(str)
  if(!Number.isInteger(num) || num < 0)
    return fail(`Unsigned integer expected, got '${str}'`)
  return ok(num)
}

function parse_number(str: string): Result<number> {
  const num = Number(str)
  if(typeof num != 'number' || isNaN(num))
    return fail(`Number expected, got '${str}'`)
  return ok(num)
}

function parse_transform_spec_list(s: string): Result<TransformSpec[]> {
  const parts = s.split('),').map(x => `${x})`)
  parts[parts.length-1] = parts[parts.length-1].slice(0, -1)
  return all(parts.map(parse_transform_spec))
}

function parse_transform_spec(s: string): Result<TransformSpec> {
  const matchObj = s.match(/^(?<source>[0-9,]*):(?<name>[a-zA-Z][a-zA-Z_-]*)\((?<args>[0-9,.-]*)\)$/)
  if(matchObj == null)
    return fail(`Invalid transform spec: ${s}`)

  const sourcesResult = all(matchObj.groups.source.split(',').map(x => parse_uint(x)))
  const argsResult = all(matchObj.groups.args.split(',').map(x => parse_number(x)))
  if(sourcesResult.status != 'ok') return fail(`Invalid transform spec "${s}" - ${sourcesResult.reason}`)
  if(argsResult.status != 'ok') return fail(`Invalid transform spec "${s}" - ${argsResult.reason}`)
  
  return ok({
    sources: sourcesResult.value, 
    name: matchObj.groups.name, 
    args: argsResult.value
  })
}

function parse_list<T>(str: string, fn: (s: string) => Result<T>): Result<T[]> {
  return all(str.split(',').map(fn))
}

function parse_val(str: string, shape) {
  let val = null
  let result = null
  if(str.length == 0) {
    return fail('Invalid argument value - empty string.')
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
    result = parse_list(str, parse_uint)
    if(result.status != 'ok') return result
    val = result.value
    break
  case 'transform-spec-list':
    result = parse_transform_spec_list(str)
    if(result.status != 'ok') return result
    val = result.value
    break
  case 'enum':
    if(shape.values.indexOf(str) == -1)
      return {status: 'invalid'}
    val = str
    break
  case 'string':
    val = str
    break
  case 'mapping-list':
    val = []
    const re = RegExp(/^[0-9]+$/)
    const strPairs: string[][] = str.split(',').map(mapstr => mapstr.split(':'))
    if(!( strPairs.every(pair => pair.length == 2) && 
          strPairs.every(([a,b]) => re.test(a) && re.test(b))))
      return {status: 'invalid'}
    val = strPairs.map(([k,v]) => [Number.parseInt(k), Number.parseInt(v)])
    break
  default:
      throw `Invalid argument type ${shape.type}`
  }
  return {status: 'ok', val}
}

function parse_opt(name: string, remaining: string[], optSchema, opts) {
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

  const parseResult = parse_val(remaining[0], shape)
  if(parseResult.status == 'ok')
    opts[shape.prop] = parseResult.val
  else
    return {status: 'err', reason: `'${remaining[0]}' is not a valid argument for ${name} - ${parseResult.reason}`}
  
  return {status: 'ok', remaining: remaining.slice(1)}
}

function parse_options(args: string[], optSchema, opts) {
  if(args.length == 0) 
    return {status: 'ok', opts}
  
  const [arg, ...remaining] = args

  if(!(arg[0] == '-' && arg.length > 1))
    return {status: 'err', reason: `Unexpected argument ${arg}`}

  const parseResult = parse_opt(arg, remaining, optSchema, opts)
  if(parseResult.status != 'ok')
    return parseResult

  return parse_options(parseResult.remaining, optSchema, opts)
}

export function parse_command(command: string, args: string[]) {
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
  const parseResult = parse_options(optArgs, cmdSchema.opts, opts)
  if(parseResult.status != 'ok')
    return parseResult
  return {status: 'ok', command, fixedArgs, opts}
}

export function parse_command_line(argList: string[]) {
  if(argList.length == 0) {
    console.log(USAGE)
    return {status: 'ok', command: null}
  }

  const [command, ...args] = argList
  return parse_command(command, args)
}