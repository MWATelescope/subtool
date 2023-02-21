/** Delay table loading, parsing, formatting and serialisation. */

import { align_float, align_int, all, apply_each, async_arrow, fail, fail_with, format_colour, is_ok, ok, read_file } from './util.js'
import type { Result, DelayTableV2, DelayTableV2Entry, AsyncResultant } from './types'

const {abs, floor, max} = Math


/*
 *  File IO
 */

export function load_delay_table(path: string): Promise<Result<DelayTableV2>> {
  return async_arrow(read_file, parse_delay_table)(path)
}


/*
 *  CSV formatting
 */

export function format_delay_table_csv(table: DelayTableV2, force_version: number = -1): string {
  const version = force_version > 0 ? force_version : table.format_version
  const format_entry = version == 1 ? format_delay_table_entry_csv_v1 : format_delay_table_entry_csv_v2
  return table.entries.map(format_entry).join('\n')
}

// TODO: do fixed cols need to be scaled?
function format_delay_table_entry_csv_v1(entry: DelayTableV2Entry): string {
  return [
    entry.rf_input,
    entry.ws_delay,
    floor(entry.initial_delay),
    floor(entry.delta_delay),
    floor(entry.delta_delta_delay),
    entry.num_pointings,
    ...entry.frac_delay.map(x => floor(x*1000))
  ].join(',')
}

function format_delay_table_entry_csv_v2(entry: DelayTableV2Entry): string {
  return [
    entry.rf_input,
    entry.ws_delay,
    entry.initial_delay,
    entry.delta_delay,
    entry.delta_delta_delay,
    entry.start_total_delay,
    entry.middle_total_delay,
    entry.end_total_delay,
    entry.num_pointings,
    entry._reserved,
    ...entry.frac_delay
  ].join(',')
}


/*
 *  Binary serialisation
 */

export function serialise_delay_table(table: DelayTableV2, force_version: number = -1): Result<ArrayBuffer> {
  const version = force_version > 0 ? force_version : table.format_version
  const row_count = table.entries.length
  const [row_length, serialise_entry] = 
    version == 1 ? [20 + 2*table.num_fracs, serialise_delay_table_entry_binary_v1]
                 : [56 + 4*table.num_fracs, serialise_delay_table_entry_binary_v2]
  const buf_length = row_count * row_length
  const buf = new ArrayBuffer(buf_length)
  const result = all(table.entries.map((entry, row_idx) => 
    serialise_entry(entry, new DataView(buf, row_idx*row_length, row_length))))
  if(!is_ok(result))
    return fail(`Error serialising delay table at row ${result.location}: ${result.reason}`)
  return ok(buf)
}

// TODO: do the fixed cols need to be scaled?
function serialise_delay_table_entry_binary_v1(entry: DelayTableV2Entry, view: DataView): Result<void> {
  if(view.byteLength < 20 + entry.frac_delay.length*2)
    return fail(`Buffer too small for delay table entry.`)
  view.setUint16(0, entry.rf_input, true)
  view.setInt16(2, entry.ws_delay, true)
  view.setInt32(4, entry.initial_delay, true) 
  view.setInt32(8, entry.delta_delay, true)
  view.setInt32(12, entry.delta_delta_delay, true) 
  view.setUint16(16, entry.num_pointings, true)
  for(let i=0; i<entry.frac_delay.length; i++)
    view.setInt16(18 + 2*i, entry.frac_delay[i] * 1000, true)
  return ok()
}

function serialise_delay_table_entry_binary_v2(entry: DelayTableV2Entry, view: DataView): Result<void> {
  if(view.byteLength < 56 + entry.frac_delay.length*4)
    return fail(`Buffer too small for delay table entry.`)
  view.setUint16(0, entry.rf_input, true)
  view.setInt16(2, entry.ws_delay, true)
  view.setFloat64(4, entry.initial_delay, true) 
  view.setFloat64(12, entry.delta_delay, true)
  view.setFloat64(20, entry.delta_delta_delay, true) 
  view.setFloat64(28, entry.start_total_delay, true) 
  view.setFloat64(36, entry.middle_total_delay, true)
  view.setFloat64(44, entry.end_total_delay, true)
  view.setUint16(52, entry.num_pointings, true)
  view.setUint16(54, entry._reserved, true)
  for(let i=0; i<entry.frac_delay.length; i++)
    view.setFloat32(56 + 4*i, entry.frac_delay[i], true)
  return ok()
}


/*
 *  Parsing
 */

export function parse_delay_table(buf: ArrayBuffer): Result<DelayTableV2> {
  if(is_text(buf))
    return parse_delay_table_csv(buf)
  else
    return parse_delay_table_binary(buf)
}

/** Parse a delay table in binary format.
 * If only the buffer is provided, the structure (i.e. version, row count, and
 * number of fractional delays) is inferred automatically, if such an inference
 * can be made unambiguously.
 * The optional arguments describe the table structure explicitly. Omitting any
 * of them will result in the values for all of them being inferred. If an
 * inferred value disagrees with a provided one, an error is returned. 
 * If all of the optional argument are supplied, their values are used as-is and
 * no inference is performed.
 */
export function parse_delay_table_binary(
    buf: ArrayBuffer, 
    version: number = -1, 
    row_count: number = -1, 
    frac_count: number = -1): Result<DelayTableV2> {

  if(version == -1 || row_count == -1 || frac_count == -1) {
    const inference = infer_binary_structure(buf)
    if(!is_ok(inference))
      return fail_with(inference)
    const [inferred_version, inferred_row_count, inferred_frac_count] = inference.value
    if(version != -1 && version != inferred_version)
      return fail(`Delay table version ${version} specified but version ${inferred_version} detected.`)
    if(row_count != -1 && row_count != inferred_row_count)
      return fail(`Delay table row count ${row_count} specified but ${inferred_row_count} rows detected.`)
    if(frac_count != -1 && frac_count != inferred_frac_count)
      return fail(`Delay table fractional delay count ${frac_count} specified but ${inferred_frac_count} fractional delays detected.`);
    ([version, row_count, frac_count] = [inferred_version, inferred_row_count, inferred_frac_count])
  }
  
  const [row_length, parse_entry] = 
    version == 1 ? [20 + frac_count * 2, parse_delay_table_entry_binary_v1]
                 : [56 + frac_count * 4, parse_delay_table_entry_binary_v2]
  const table: DelayTableV2Entry[] = []
  for(let row_idx=0; row_idx<row_count; row_idx++) {
    const row = new DataView(buf, row_idx*row_length, row_length)
    const entry = parse_entry(row)
    if(!is_ok(entry))
      return fail_with(entry)
    table.push(entry.value)
  }
  return ok({
    format_version: version,
    num_fracs: frac_count,
    entries: table,
  })
}

// TODO: do these fixed cols need scaling?
function parse_delay_table_entry_binary_v1(view: DataView): Result<DelayTableV2Entry> {
  const num_fracs = (view.byteLength-20) / 2
  return ok({
    rf_input: view.getUint16(0, true),
    ws_delay: view.getInt16(2, true),
    initial_delay: view.getInt32(4, true),
    delta_delay: view.getInt32(8, true),
    delta_delta_delay: view.getInt32(12, true),
    start_total_delay: 0,
    middle_total_delay: 0,
    end_total_delay: 0,
    num_pointings: view.getInt16(16, true),
    _reserved: 0,
    frac_delay: new Float32Array(num_fracs).map((_,i) => view.getInt16(18+2*i, true)/1000)
  })
}

function parse_delay_table_entry_binary_v2(view: DataView): Result<DelayTableV2Entry> {
  if(view.byteLength < 56)
    return fail(`Binary delay table entry is too short.`)
  const num_fracs = (view.byteLength-56) / 4
  return ok({
    rf_input: view.getUint16(0, true),
    ws_delay: view.getInt16(2, true),
    initial_delay: view.getFloat64(4, true), 
    delta_delay: view.getFloat64(12, true),
    delta_delta_delay: view.getFloat64(20, true), 
    start_total_delay: view.getFloat64(28, true),
    middle_total_delay: view.getFloat64(36, true),
    end_total_delay: view.getFloat64(44, true),
    num_pointings: view.getUint16(52, true),
    _reserved: view.getUint16(54, true),
    frac_delay: new Float32Array(num_fracs).map((_,i) => view.getFloat32(56+4*i, true)),
  })
}

function parse_delay_table_csv(buf: ArrayBuffer): Result<DelayTableV2> {
  const csv = parse_csv(buf)
  const first_bad_row = is_rectangular(csv)
  if(first_bad_row > -1)
    return fail(`Bad CSV file: table is not rectangular. Line 0 has ${csv[0].length} cols vs. ${csv[first_bad_row].length} cols in line ${first_bad_row}.`)
  const detection = detect_csv_version(csv)
  if(!is_ok(detection))
    return fail_with(detection)
  const parser = detection.value == 1 ? parse_delay_table_entry_csv_v1 : parse_delay_table_entry_csv_v2
  const result = all(csv.map(parser))
  if(!is_ok(result)) {
    const r = result as {location:string[]}
    return fail(`Error parsing CSV at row ${r.location[0]}, col ${r.location[1]}: ${result.reason}`)
  }
  const num_fracs = result.value[1].frac_delay.length
  console.warn(`Detected V${detection.value} CSV delay table with ${result.value.length} sources and ${num_fracs} fractional delays.`)
  return ok({
    entries: result.value,
    format_version: detection.value,
    num_fracs,
  })
}

function parse_delay_table_entry_csv_v1(row: string[]): Result<DelayTableV2Entry> {
  const fixedParsers = [parse_uint, parse_int, parse_int, parse_int, parse_int, parse_uint]
  const result = all([
    ...apply_each(row.slice(0, 6), fixedParsers),
    ...row.slice(6).map(parse_float)
  ])
  if(!is_ok(result))
    return fail_with(result)
  else return ok({
    rf_input: result.value[0],
    ws_delay: result.value[1],
    initial_delay: result.value[2],
    delta_delay: result.value[3],
    delta_delta_delay: result.value[4],
    start_total_delay: 0,
    middle_total_delay: 0,
    end_total_delay: 0,
    num_pointings: result.value[5],
    _reserved: 0,
    frac_delay: Float32Array.from(result.value.slice(6)).map(x => x/1000)
  })
}

function parse_delay_table_entry_csv_v2(row: string[]): Result<DelayTableV2Entry> {
  const fixedParsers = [parse_uint, parse_int, parse_float, parse_float, parse_float, 
    parse_float, parse_float, parse_float, parse_uint, parse_uint]
  const result = all([
    ...apply_each(row.slice(0, 10), fixedParsers),
    ...row.slice(10).map(parse_float)
  ])
  if(!is_ok(result))
    return fail_with(result)
  else return ok({
    rf_input: result.value[0],
    ws_delay: result.value[1],
    initial_delay: result.value[2],
    delta_delay: result.value[3],
    delta_delta_delay: result.value[4],
    start_total_delay: result.value[5],
    middle_total_delay: result.value[6],
    end_total_delay: result.value[7],
    num_pointings: result.value[8],
    _reserved: result.value[9],
    frac_delay: Float32Array.from(result.value.slice(10))
  })
}


/*
 *  Format detection
 */

/** Looks for all ones in the two possible num_pointings columns. */
function detect_csv_version(csv: string[][]): Result<number> {
  if(csv.every(row => row[5] == '1'))
    return ok(1)
  else if(csv.every(row => row[8] == '1'))
    return ok(2)
  return fail(`Bad CSV file: unable to detect version - couldn't find a num_pointings column containing all ones.`)
}

/** Attempt to detect which version of binary delay table is in the buffer. */
function detect_binary_version(buf: ArrayBuffer): Result<number> {
  const plausibly_v1 = is_plausible_binary_v1(buf)
  const plausibly_v2 = is_plausible_binary_v2(buf)
  if(plausibly_v1 && plausibly_v2)
    return fail(`Failed to detect delay table binary version: ambiguous structure.This is probably a bug in subtool, please report!`)
  else if(plausibly_v1)
    return ok(1)
  else if(plausibly_v2)
    return ok(2)
  else
    return fail(`Failed to detect delay table binary version: did not match any known structure. This is probably a bug in subtool, please report!`)
}

/** Test if the buffer appears to contain a valid version 1 binary table. */
function is_plausible_binary_v1(buf: ArrayBuffer): boolean {
  const view = new DataView(buf)
  const initialDelay = view.getInt32(4, true) 
  const numPointings = view.getUint16(16, true)
  const firstFrac = view.getInt16(18, true)
  
  return numPointings == 1
      && abs(initialDelay - firstFrac) < 0.0001
      && abs(firstFrac) <= 2000
      && ((initialDelay == 0)  == (firstFrac == 0))
}

/** Test if the buffer appears to contain a valid version 2 binary table. */
function is_plausible_binary_v2(buf: ArrayBuffer): boolean {
  const view = new DataView(buf)
  const initialDelay = view.getFloat64(4, true) 
  const startDelay = view.getFloat64(28, true) 
  const numPointings = view.getUint16(52, true)
  const reserved = view.getUint16(54, true)
  const firstFrac = view.getFloat32(56, true)
  
  return numPointings == 1
      && reserved == 0
      && abs(initialDelay - startDelay) < 0.0001
      && abs(initialDelay - firstFrac) < 0.0001
}

type FracType = 'int16' | 'float32'

/** Determine the type and dimensions of a binary delay table.
 * 
 * Returns a tuple of version number, row count and number of fractional delays.
 */
function infer_binary_structure(buf: ArrayBuffer): Result<[number, number, number]> {
  const detection = detect_binary_version(buf)
  if(!is_ok(detection))
    return fail_with(detection)
  const [frac_type, frac_offset, np_offset, resv_offset, row_padding]: 
        [FracType, number, number, number, number] = 
    detection.value == 1 ? ['int16',   18, 16, -2, 2]
                         : ['float32', 56, 52, 54, 0]
  const max_rows = buf.byteLength / (frac_offset + row_padding)
  const view = new DataView(buf)
  for(let row_count=0; row_count<=max_rows; row_count++) {
    const check = is_viable_binary_structure(row_count, frac_type, frac_offset, np_offset, resv_offset, row_padding, view)
    if(!is_ok(check))
      continue
    const frac_count = check.value
    console.warn(`Detected V${detection.value} binary delay table with ${row_count} sources and ${frac_count} fractional delays.`)
    return ok([detection.value, row_count, frac_count])
  }
  return fail('Failed to determine binary delay table structure.')
}

/** Check to see if a binary delay table matches the specified structure.
 * 
 * row_count    the number of row in the table.
 * frac_type    fractional delay type ('int16' or 'float32').
 * frac_offset  offset of first fractional delay.
 * np_offset    offset of num_pointings column which should contain all ones.
 * resv_offset  offset of reserved column which should contain all zeroes (use
 *              negative offset of row padding byte for v1 table).
 * row_padding  byte length of padding at end of row.
 * 
 * Checks the following conditions:
 *  1. The row count evenly divides the byte length of the table.
 *  2. The fractional delay size evenly divides the implied row length minus
 *     padding and fixed columns.
 *  2. The value in the num_pointings column is always 1.
 *  3. The value in the reserved/padding column is always 0.
 *  4. Fractional delays are always in the range equivalent to +- 2 samples.
 * 
 * Succeeds with the implied number of fractional delays, or fails.
 * TODO: use an option type instead.
 */
function is_viable_binary_structure(
    row_count:   number,
    frac_type:   FracType,
    frac_offset: number,
    np_offset:   number,
    resv_offset: number,
    row_padding: number,
    view:        DataView): Result<number> {
  
 if(view.byteLength % row_count != 0)
   return fail('')
  const row_length = view.byteLength / row_count
  const get_frac = frac_type == 'int16' ? p => view.getInt16(p, true) : p => view.getFloat32(p, true)
  const frac_size = frac_type == 'int16' ? 2 : 4
  const frac_area = row_length - row_padding - frac_offset
  if(frac_area % frac_size != 0)
    return fail('')
  const frac_count = frac_area / frac_size
  
  if(resv_offset < 0) resv_offset = row_length - resv_offset
  for(let row=0; row<row_count; row++) {
    const row_offset = row * row_length
    const num_pointings = view.getUint16(row_offset + np_offset, true)
    const reserved = view.getUint16(row_offset + resv_offset, true)
    if(num_pointings != 1 || reserved != 0)
      return fail('')
    for(let frac_id=0; frac_id<frac_count; frac_id++) {
      const frac = get_frac(row_offset + frac_offset + frac_size * frac_id)
      const in_range = frac_type == 'int16' 
                     ? (frac >= -2000 && frac <= 2000) 
                     : (frac >= -2 && frac <= 2)
      if(!in_range)
        return fail('')
    }
  }
  
  return ok(frac_count)
}


/*
 *  Pretty printing
 */

/** Pretty-print the delay table. 
 * TODO: print currently-unused columns
 */
export function format_delay_table_pretty(
    table: DelayTableV2,
    frac_digits = 6, 
    use_colour = true,
    force_version: number = -1,
    allow_wrap = false,
    ): string {

  const version = force_version > 0 ? force_version : table.format_version
  const source_id_width = max(...table.entries.map(row => row.rf_input.toString().length))
  const ws_delay_width = max(...table.entries.map(row => row.ws_delay.toString().length))
  const frac_width = version == 1 ? 5 : frac_digits + 3
  const frac_precision = version == 1 ? 0 : frac_digits
  const max_width = allow_wrap ? Infinity : process.stdout.columns
  const fixed_width = source_id_width + ws_delay_width + 2
  const fracs_width = max_width - fixed_width
  const num_fracs = floor(fracs_width / (frac_width + 1))
  const colourise = use_colour ? (colour: 'yellow'|'cyan', x: string) => format_colour(colour, x) : (_:any,x:string) => x
  const rescale = version == 1 ? (x:number) => x * 1000 : (x:number) => x
  return table.entries.map(entry => [
      colourise('cyan', align_int(source_id_width, entry.rf_input)),
      colourise('yellow', align_int(ws_delay_width, entry.ws_delay)),
      Array.from(entry.frac_delay.slice(0, num_fracs))
        .map(x => align_float(frac_width, frac_precision, rescale(x)))
        .join(' ')
    ].join(' ')).join('\n')
}

/*
 *  Manipulation
 */

export function delay_table_subset(source_ids: number[], table: DelayTableV2): DelayTableV2 {
  return {...table, entries: table.entries.filter(x => source_ids.includes(x.rf_input))}
}


/*
 *  General utility functions
 */

/** Parse the whole string as a (possibly signed) integer. */
function parse_int(str: string): Result<number> {
  if(str.match(/^-?\d+$/) == null)
    return fail(`Failed to parse integer: "${str}"`)
  return ok(Number.parseInt(str))
}

/** Parse the whole string as an unsigned integer. */
function parse_uint(str: string): Result<number> {
  if(str.match(/^\d+$/) == null)
    return fail(`Failed to parse unsigned integer: "${str}"`)
  return ok(Number.parseInt(str))
}

/** Parse the whole string as a float. */
function parse_float(str: string): Result<number> {
  if(str.match(/^-?\d+(\.\d+)?$/) == null)
    return fail(`Failed to parse float: "${str}"`)
  return ok(Number.parseFloat(str))
}

/** True if any cell in a 2D string array includes a period character. */
function has_decimals(xxs: string[][]): boolean {
  return xxs.some(xs => xs.some(x => x.includes('.')))
}

/** Test if a 2D array has the same number of columns in every row.
 * Returns -1 if all the rows in the 2D array are the same length or if the
 * array is empty, otherwise returns the index of first row to be of a
 * different length to the first.
 */
function is_rectangular(xxs: any[][]): number {
  if(xxs.length == 0) 
    return -1
  const n = xxs[0].length
  return xxs.findIndex(xs => xs.length != n)
}

/** Parse an ArrayBuffer as CSV. Trailing whitespace is ignored. */
function parse_csv(buf: ArrayBuffer): string[][] {
  const str = new TextDecoder().decode(buf).trimEnd()
  return split_lines(str).map(line => line.split(','))
}

/** Split a string on LF or CRLF. */
function split_lines(str: string): string[] {
  return str.split(/\r?\n/)
}

/** True if the buffer contains only ASCII printable characters, tabs, and CR/LF. */
function is_text(buf: ArrayBuffer): boolean {
  const xs = new Uint8Array(buf)
  return xs.every(is_printable)
}

/** True if the value is an ASCII printable character, a tab, or CR/LF. */
function is_printable(c: number): boolean {
  return (c >= 0x20 && c <= 0x7E) // Printable
      || c == 0x09                // Tab
      || c == 0x0A                // LF
      || c == 0x0D                // CR
}


/*
 * Old stuff to fix or delete
 */

/** Calculate the relative delays for translating one delay table to another.
 * 
 * Takes two delay table objects and returns a new one. Differences are also
 * calculated for initial and delta values, for informational purposes, but not
 * `num_pointings` which is left as 1 as this constant is useful for shape
 * auto-detection and not much else. The source IDs must be the same in both
 * tables, and in the same order.
 */
export function compare_delay_tables(from: DelayTableV2, to: DelayTableV2): Result<DelayTableV2> {
  if(from.entries.length != to.entries.length)
    return fail(`Delay table length mismatch: ${from.entries.length} != ${to.entries.length}.`)
  for(let i=0; i<from.entries.length; i++)
    if(from.entries[i].rf_input != to.entries[i].rf_input)
      return fail(`Source ID mismatch in delay tables at row ${i}: ${from.entries[i].rf_input} != ${to.entries[i].rf_input}.`)
  
  const entries: DelayTableV2Entry[] = to.entries.map((row,i) => ({
    rf_input: row.rf_input,
    ws_delay: row.ws_delay - from.entries[i].ws_delay,
    initial_delay: row.initial_delay - from.entries[i].initial_delay,
    delta_delay: row.delta_delay - from.entries[i].delta_delay,
    delta_delta_delay: row.delta_delta_delay - from.entries[i].delta_delta_delay,
    num_pointings: row.num_pointings - from.entries[i].num_pointings,
    start_total_delay: row.start_total_delay - from.entries[i].start_total_delay,
    middle_total_delay: row.middle_total_delay - from.entries[i].middle_total_delay,
    end_total_delay: row.end_total_delay - from.entries[i].end_total_delay,
    _reserved: row._reserved - from.entries[i]._reserved,
    frac_delay: row.frac_delay.map((x, j) => x - from.entries[i].frac_delay[j]),
  }))
  return ok({...to, entries})
}

//
//function flip_fracs(table: DelayTable): DelayTable {
//  return table.map(row => ({ ...row, frac_delay: row.frac_delay.map(x => -x) }))
//}


/** Clone a delay table. */
export function clone_delay_table(dt: DelayTableV2): DelayTableV2 {
  return {...dt, entries: dt.entries.map(clone_delay_table_entry)}
}

/** Clone a delay table entry. */
export function clone_delay_table_entry(dte: DelayTableV2Entry): DelayTableV2Entry {
  return {...dte, frac_delay: dte.frac_delay.slice()}
}

