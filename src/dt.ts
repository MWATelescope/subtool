/** Delay table loading, parsing, formatting and serialisation. */

import * as fs from 'node:fs/promises'
import { fail, ok } from './util.js'
import { read_section } from './reader.js'
import type { Metadata, DelayTableEntry, DelayTable, Result } from './types'
import { FileHandle } from 'node:fs/promises'
import {Cache} from './cache.js'

/*
 *    PARSING
 */

export function parse_delay_table_binary(buf: ArrayBuffer, meta: Metadata, byteOffset=0): Result<DelayTable> {
  //if(!Number.isInteger(meta.num_sources) || !Number.isInteger(meta.num_frac_delays))
  //  return {status: 'err', reason: `Internal error: Can't parse binary delay table before dimensions have been determined. ${meta.num_sources} ${meta.num_frac_delays}  ` }

  let table = []
  const dtpad = meta.frac_delay_size == 2 ? 2 : 0 // FIXME: hack
  const rowLen = 18 + dtpad + meta.frac_delay_size*meta.num_frac_delays
  for(let i=0; i<meta.num_sources; i++) {
    const view = new DataView(buf, i * rowLen + byteOffset, rowLen)
    const row = parse_delay_table_row(view, meta)
    table.push(row)
  }

  return ok(table)
}

export function parse_delay_table_row(view: DataView, meta: Metadata): DelayTableEntry {
  const parseFrac = meta.frac_delay_size == 4 ? i => view.getInt32(18+4*i, true)
                  : meta.frac_delay_size == 2 ? i => 1000 * view.getInt16(18+2*i, true)
                  : _ => { throw `Unsupported frac delay size ${meta.frac_delay_size}.` }

  return {
    rf_input: view.getUint16(0, true),
    ws_delay: view.getInt16(2, true),
    initial_delay: view.getInt32(4, true),
    delta_delay: view.getInt32(8, true),
    delta_delta_delay: view.getInt32(12, true),
    num_pointings: view.getInt16(16, true),
    frac_delay: new Int32Array(meta.num_frac_delays).map((_,i) => parseFrac(i))
  }
}

export function parse_delay_table_csv(csv: string, frac_delay_size: number): Result<DelayTable> {
  if(frac_delay_size == null)
    console.warn(`Reading CSV delay table with unknown fractional delay precision. Will guess from values.`)

  const table = csv.trim().split('\n').map(line => { 
    const [ 
      rf_input, 
      ws_delay, 
      initial_delay, 
      delta_delay,
      delta_delta_delay, 
      num_pointings,
      ...frac_delay] = line.split(',').map(x => Number.parseInt(x))
    return { rf_input, ws_delay, initial_delay, delta_delay, delta_delta_delay, num_pointings, 
      frac_delay: Int32Array.from(frac_delay) }
  })

  const allUnder2k = table.every(row => row.frac_delay.every(x => Math.abs(x) < 2000))
  if(allUnder2k) {
    console.warn(`Detected CSV file with absolute values for all fractional delays less than 2000. Assuming millisamples.`)
    for(let row of table)
      row.frac_delay = row.frac_delay.map(x => x * 1000)
  } else {
    console.warn(`Detected CSV file with absolute values for fractional delays exceeding 2000. Assuming microsamples.`)
  }

  return ok(table)
}

/*
 *    OUTPUT FORMATTING
 */

export function print_delay_table(tiles: DelayTable, delayTableBuf: ArrayBuffer, opts, meta: Metadata): void {
  const scaleFrac = meta.frac_delay_size == 2 ? x => x / 1000
                  : x => x // FIXME: hack
  if(opts.format_out == 'pretty') {
    if(opts.selected_sources != null)
      tiles = tiles.filter(x => opts.selected_sources.indexOf(x.rf_input) != -1)
    const pad = (n, x) => x.toString().padStart(n, ' ')
    const padf = (n, x) => x >= 0 ? ' ' + x.toString().slice(0,n-2).padEnd(n-1, ' ') : x.toString().slice(0,n-2).padEnd(n, ' ')
    console.log("Tile metadata:")
    for(let tile of tiles) {
      const num_frac_delays = opts.num_frac_delays ?? tile.frac_delay.length
      console.log([
        pad(4, tile.rf_input),
        pad(3, tile.ws_delay),
        pad(14, tile.initial_delay), // / 1048576000),
        pad(5, tile.delta_delay),
        tile.delta_delta_delay,
        tile.num_pointings,
        tile.frac_delay.slice(0,num_frac_delays).map(scaleFrac).toString(),
        ].join(' ') + (num_frac_delays >= tile.frac_delay.length ? '' : '...')
      )
    } 
  } else if(opts.format_out == 'csv') {
    if(opts.selected_sources != null)
      tiles = tiles.filter(x => opts.selected_sources.indexOf(x.rf_input) != -1)
    for(let tile of tiles) {
      const num_frac_delays = opts.num_frac_delays ?? tile.frac_delay.length
        console.log([
          tile.rf_input,
          tile.ws_delay,
          tile.initial_delay,
          tile.delta_delay,
          tile.delta_delta_delay,
          tile.num_pointings,
          tile.frac_delay.slice(0,num_frac_delays).map(scaleFrac).join(','),
          ].join(',')
        )
    }
  } else if(opts.format_out == 'bin') {
    const num_frac_delays = opts.num_frac_delays ?? tiles[0].frac_delay.length

    
    if(delayTableBuf && opts.selected_sources == null && num_frac_delays == tiles[0].frac_delay.length) {
      // If there are no constraints, print the original binary (if avaiilable).
      process.stdout.write(Buffer.from(delayTableBuf))
    } else {
      // Otherwise, (re)build the binary structure
      if(opts.selected_sources != null || num_frac_delays != tiles[0].frac_delay.length)
        console.warn("Warning: binary output with selection constraints requires the table to be reconstructed with different dimensions and is likely to be incompatible with the original subfile.")
      if(opts.selected_sources != null)
        tiles = tiles.filter(x => opts.selected_sources.indexOf(x.rf_input) != -1)
      const buf = serialise_delay_table(tiles, tiles.length, num_frac_delays, meta.frac_delay_size)
      process.stdout.write(Buffer.from(buf))
    }
  } else {
    throw `Unsupported output format for delay table: ${opts.format_out}`
  }
}

export function serialise_delay_table(table: DelayTable, num_sources: number, num_fracs: number, frac_size: number): ArrayBuffer {
  if(table.length != num_sources)
    console.warn(`WARNING: Creating binary delay table with ${num_sources} sources, but ${table.length} sources were provided.`)
  if(!table.reduce((acc, x) => acc && x.frac_delay.length == table[0].frac_delay.length, true))
    throw 'Invalid delay table: fractional delays length mismatch detected'
  if(table.length > 1 && table[0].frac_delay.length != num_fracs)
    console.warn(`WARNING: Creating binary delay table with ${num_fracs} fractional delays, but the provided table has ${table[0].frac_delay.length} fractional delays.`)
  const dtpad = frac_size == 2 ? 2 : 0
  const rowLen = 18 + dtpad + frac_size*num_fracs
  const buf = new ArrayBuffer(num_sources * rowLen)
  const setFrac = frac_size == 2 ? (view, x, j) => view.setInt16(18 + j*2, x/1000, true)
                : frac_size == 4 ? (view, x, j) => view.setInt32(18 + j*4, x, true)
                : (view, x, j) => { throw `Invalid frac delay size ${frac_size}.` }
  table.slice(0, num_sources).forEach((tile, i) => {
    const view = new DataView(buf, i * rowLen, rowLen)
    view.setUint16(0, tile.rf_input          , true)
    view.setInt16(2,  tile.ws_delay          , true)
    view.setInt32(4,  tile.initial_delay     , true)
    view.setInt32(8,  tile.delta_delay       , true)
    view.setInt32(12, tile.delta_delta_delay , true)
    view.setInt16(16, tile.num_pointings     , true)
    tile.frac_delay.slice(0, num_fracs).forEach((x,j) => setFrac(view, x, j))
  })
  return buf
}


/*
 *    FILE LOADING
 */

/** Read the delay table section from an open subfile. */
export async function read_delay_table(file: FileHandle, meta: Metadata, cache: Cache): Promise<Result<DelayTable>> {
  const sectionResult = await read_section('dt', file, meta, cache)
  if(sectionResult.status != 'ok')
    return fail(sectionResult.reason)

  const parseResult = parse_delay_table_binary(sectionResult.value, meta)
  if(parseResult.status != 'ok')
    return fail(parseResult.reason)

  return ok(parseResult.value)
}

/** Load a delay table from a CSV file. */
export async function load_delay_table_csv(filename: string, opts, meta: Metadata): Promise<Result<DelayTable>> {
  const csv = await fs.readFile(filename, 'utf8')
  const parseResult = parse_delay_table_csv(csv, meta.frac_delay_size)
  if(parseResult.status != 'ok')
    return fail(parseResult.reason)
  const table = parseResult.value
  const num_sources = table.length
  const num_frac_delays = table[0].frac_delay.length
  
  if(Number.isInteger(meta.num_frac_delays) && meta.num_frac_delays != num_frac_delays)
    return fail(`Expected number of fractional delays from metadata (${meta.num_frac_delays}) inconsistent with number found in file (${num_frac_delays}).`)
  if(Number.isInteger(meta.num_sources) && meta.num_sources != num_sources)
    return fail(`Expected number of sources from metadata (${meta.num_sources}) inconsistent with number found in file (${num_sources}).`)
  if(Number.isInteger(opts.num_frac_delays) && opts.num_frac_delays != num_frac_delays)
    return fail(`Expected number of fractional delays from options (${opts.num_frac_delays}) inconsistent with number found in file (${num_frac_delays}).`)
  if(Number.isInteger(opts.num_sources) && opts.num_sources != num_sources)
    return fail(`Expected number of sources from options (${opts.num_sources}) inconsistent with number found in file (${num_sources}).`)

  if(!Number.isInteger(meta.num_sources) || !Number.isInteger(meta.num_frac_delays)) {
    console.warn(`Delay table loaded with ${num_sources} sources and ${num_frac_delays} fractional delays.`)
    meta.num_sources = num_sources
    meta.num_frac_delays = num_frac_delays
  }

  return ok(table)
}
type LoadDelayTableBinaryResult = {
  table: DelayTable,
  opts: any,
  meta: Metadata,
  binaryBuffer: ArrayBuffer,
}

export async function load_delay_table_binary(filename: string, opts, meta: Metadata): Promise<Result<LoadDelayTableBinaryResult>> {
  if(meta.frac_delay_size == null)
    return fail(`Internal error: frac delay size must be known before loading binary delay table.`)
  const dtpad = meta.frac_delay_size == 2 ? 2 : 0
  const buf = await fs.readFile(filename)
  // First, figure out if we already have enough information to determine the shape and try to fill
  // in missing details.
  if(meta.num_sources == null && meta.num_frac_delays != null) {
    const impliedRowLen = 18 + dtpad + meta.frac_delay_size*meta.num_frac_delays
    const impliedRowCount = buf.byteLength / impliedRowLen

    if(buf.byteLength % impliedRowCount != 0) 
      return fail(`Number of fractional delays ${meta.num_frac_delays} is inconsistent with binary delay table size ${buf.byteLength}.`)

    meta.num_sources = impliedRowCount
  } else if(meta.num_frac_delays == null && meta.num_sources != null) {
    if(buf.byteLength % meta.num_sources != 0) 
      return fail(`Number of sources ${meta.num_sources} is inconsistent with binary delay table size ${buf.byteLength}.`)

    const impliedRowLen = buf.byteLength / meta.num_sources
    const impliedFracDelays = (impliedRowLen - 18 - dtpad) / meta.frac_delay_size
    meta.num_frac_delays = impliedFracDelays
  } else {
    console.warn(`Attempting to load binary delay table with unspecified dimensions. If known, dimensions may be specified with --num-sources and --num-frac-delays.`)
    const result = detect_delay_table_shape_binary(buf.buffer, filename, meta.frac_delay_size)
    if(result.status != 'ok')
      return fail(result.reason)
    meta.num_sources = result.num_sources
    meta.num_frac_delays = result.num_frac_delays
  }

  const parseResult = parse_delay_table_binary(buf.buffer, meta)
  if(parseResult.status != 'ok')
    return fail(parseResult.reason)

  return ok({table: parseResult.value, opts, meta, binaryBuffer: buf})
}

/** Load a delay table from a file.
 * The file type (CSV or binary) is determined by `opts.format_in`, or auto-detected if null. The
 * shape is determined by `meta.num_sources` and `meta.num_frac_delays`, or auto-detected if both
 * are null. If specified, these values must be consistent with the actual file. Returns a status
 * object.
 */
export async function load_delay_table(filename: string, opts, meta: Metadata): Promise<Result<DelayTable>> {
  if(opts.format_in == 'csv')
    return load_delay_table_csv(filename, opts, meta)
  else if(opts.format_in == 'bin') {
    const result = await load_delay_table_binary(filename, opts, meta)
    if(result.status != 'ok')
      return fail(result.reason)
    return ok(result.value.table)
  } else if(opts.format_in == 'auto') {
    const format = await detect_delay_table_format(filename)
    if(format == 'csv') {
      console.warn(`Detected CSV encoding for delay table file: '${filename}'.`)
      return load_delay_table_csv(filename, opts, meta)
    } else if(format == 'bin') {
      console.warn(`Detected binary encoding for delay table file: '${filename}'.`)
      const result = await load_delay_table_binary(filename, opts, meta)
      if(result.status != 'ok')
        return fail(result.reason)
      return ok(result.value.table)
    } else
      return fail(`Could not detect delay table format for: ${filename}`)
  } else
    return fail(`Internal error: format_in not specified for: ${filename}`)
}

/*
 *   AUTO-DETECTION
 */

/** Detect the format (binary or CSV) of a delay table file. 
 * Returns a string: 'csv', 'bin', or 'unknown'. 
 */
 export async function detect_delay_table_format(filename) {
  const buf = await fs.readFile(filename)
  if(buf.includes('\x01'))
    return 'bin'
  else if(buf.includes(','))
    return 'csv'
  else
    return 'unknown'
}

/** Detect the number of sources and fractional delays of a binary delay table, given an ArrayBuffer. 
 * Returns a status object.
 */
export function detect_delay_table_shape_binary(buf: ArrayBuffer, filename: string, frac_size: number) {
  if(buf.byteLength < 20)
    return {status: 'err', reason: `File is too small to be a valid binary delay table: ${filename}`}

  const pad = frac_size == 2 ? 2 : 0 // FIXME: hack

  /** Check if a candidate number of fractional delays is plausible. */
  function isViable(nFracs) {
    const impliedRowLen = 18 + pad + frac_size*nFracs
    const impliedRowCount = buf.byteLength / impliedRowLen

    // Test 1: The implied row length must divide the file evenly.
    if(buf.byteLength % impliedRowLen != 0)
      return false
    const getFrac = frac_size == 2 ? (view, n) => view.getInt16(18 + n*2, true)
                  : frac_size == 4 ? (view, n) => view.getInt32(18 + n*4, true)
                  : (view, n) => { throw `Unsupported fractional delay size ${frac_size}.` }
    const maybeSourceIds = []    
    for(let i=0; i<impliedRowCount; i++) {
      const view = new DataView(buf, i*impliedRowLen, impliedRowLen)
      
      const maybeFirstFrac = getFrac(view, 0)
      const maybeLastFrac = getFrac(view, nFracs-1)
      const maybeOverallSign = Math.sign(maybeLastFrac - maybeFirstFrac)

      // Test 2: For now, `num_pointings` must always be 1.
      const maybeNumPointings = view.getInt16(16, true)
      if(maybeNumPointings != 1)
        return false

      // Test 3: Source IDs must be unique.
      const maybeSourceId = view.getUint16(0, true)
      if(maybeSourceIds.indexOf(maybeSourceId) != -1)
        return false
      else
        maybeSourceIds.push(maybeSourceId)

      for(let j=0; j<nFracs; j++) {
        const maybeFrac = getFrac(view, j)

        // Test 4: Fractional delays must be in the range [-2000,2000]
        if(maybeFrac < -2000 || maybeFrac > 2000)
          return false

        // Test 5: Fractional delays must increment monotonically
        if(j > 0) {
          const maybeLastFrac = getFrac(view, j-1)
          const maybeSign = Math.sign(maybeFrac - maybeLastFrac)
          if(maybeSign != 0 && maybeSign != maybeOverallSign)
            return false
        }
      }   
    }
    return true
  }
  const maxPossibleFracs = (buf.byteLength - 18 - pad) / frac_size
  let numFracs = 0
  let foundPlausibleValue = false
  while(numFracs < maxPossibleFracs) {
    if(isViable(numFracs)) {
      foundPlausibleValue = true
      break
    }
    numFracs++
  }
  if(!foundPlausibleValue)
    return {status: 'err', reason: 'Unable to determine shape of binary delay table.'}
  
  const rowLen = 18 + pad + frac_size*numFracs
  const rowCount = buf.byteLength / rowLen
  console.warn(`Detected ${rowCount} sources and ${numFracs} fractional delays.`)
  return {status: 'ok', num_sources: rowCount, num_frac_delays: numFracs}
}

/** Calculate the relative delays for translating one delay table to another.
 * 
 * Takes two delay table objects and returns a new one. Differences are also
 * calculated for initial and delta values, for informational purposes, but not
 * `num_pointings` which is left as 1 as this constant is useful for shape
 * auto-detection and not much else. The source IDs must be the same in both
 * tables, and in the same order.
 */
export function compare_delays(from, to) {
  if(from.length != to.length)
    return {status: 'err', reason: `Delay table length mismatch: ${from.length} != ${to.length}.`}
  for(let i=0; i<from.length; i++)
    if(from[i].rf_input != to[i].rf_input)
      return {status: 'err', reason: `Source ID mismatch in delay tables at row ${i}: ${from[i].rf_input} != ${to[i].rf_input}.`}
  
  const table = to.map((row,i) => ({
    rf_input: row.rf_input,
    ws_delay: row.ws_delay - from[i].ws_delay,
    initial_delay: row.initial_delay - from[i].initial_delay,
    delta_delay: row.delta_delay - from[i].delta_delay,
    delta_delta_delay: row.delta_delta_delay - from[i].delta_delta_delay,
    num_pointings: row.num_pointings - from[i].num_pointings,
    frac_delay: row.frac_delay.map((x, j) => x - from[i].frac_delay[j]),
  }))
  return {status: 'ok', table}
}

function flip_fracs(table: DelayTable): DelayTable {
  return table.map(row => ({ ...row, frac_delay: row.frac_delay.map(x => -x) }))
}