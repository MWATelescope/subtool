import { FileHandle } from 'fs/promises'
import * as fs from 'node:fs/promises'
import type { AsyncResultant, ErrorLocation, Metadata, Result, Resultant, Z } from './types'

const {round} = Math

/** Create a new metadata object, used for tracking information about files. */
export function init_metadata(): Metadata { 
  return {
    filename: null,          
    filetype: null,          
    observation_id: null,    
    subobservation_id: null, 
    num_sources: null,       
    num_frac_delays: null,   
    frac_delay_size: null,
    sample_rate: null,       
    secs_per_subobs: null,   
    samples_per_line: null,  
    samples_per_packet: null,
    udp_payload_length: null,
    udp_per_rf_per_sub: null,
    sub_line_size: null,     
    blocks_per_sub: null,    
    blocks_per_sec: null,
    fft_per_block: null,     
    block_length: null,      
    margin_packets: null,    
    margin_samples: null,    
    dt_present: null,        
    dt_offset: null,         
    dt_length: null,         
    header_present: null,    
    header_offset: null,     
    header_length: null,     
    data_present: null,      
    data_offset: null,       
    data_length: null,       
    margin_present: null,    
    margin_offset: null,     
    margin_length: null,     
    udpmap_present: null,    
    udpmap_offset: null,     
    udpmap_length: null,     
    sources: null,      
    delay_table: null,    
    mwax_sub_version: null, 
    dt_entry_min_size: null,
  }
}

/** Read section data from a subfile into an ArrayBuffer. */
export async function read_section(name: string, file: FileHandle, meta: Metadata) {
  const isPresent = meta[`${name}_present`]
  const length = meta[`${name}_length`]
  const position = meta[`${name}_offset`]

  if(!isPresent)
    return {status: 'err', reason: `Cannot read section '${name}' when not indicated in metadata.`}

  const buf = new ArrayBuffer(length)
  const result = await file.read(new Uint8Array(buf), 0, length, position)
  if(result.bytesRead != length)
    return {status: 'err', reason: `Failed to read section '${name}'. Expected to read ${length} bytes, got ${result.bytesRead}`}

  return {status: 'ok', buf}
}

/** Read a whole block of data from a subfile into an ArrayBuffer. */
export async function read_block(num: number, file: FileHandle, meta: Metadata) {
  if(!meta.data_present)
    return {status: 'err', reason: `Cannot read data section when not indicated in metadata.`}
  if(num < 0)
    return {status: 'err', reason: `Invalid block number ${num}.`}
  if(num > meta.blocks_per_sub)
    return {status: 'err', reason: `Cannot read block ${num} when metadata indicates only ${meta.blocks_per_sub} blocks per subfile.`}
  
  const length = meta.block_length
  const position = meta.header_length + num * meta.block_length

  const buf = new ArrayBuffer(length)
  const result = await file.read(new Uint8Array(buf), 0, length, position)
  if(result.bytesRead != length)
    return {status: 'err', reason: `Failed to read block '${num}'. Expected to read ${length} bytes, got ${result.bytesRead}`}

  return {status: 'ok', buf}
}

/** Read the margin section from a subfile. */
export async function read_margin(file, meta) {
  const sectionResult = await read_section('margin', file, meta)
  if(sectionResult.status != 'ok')
    return sectionResult

  return {status: 'ok', buf: sectionResult.buf}
}

export async function write_section(name: string, buffer: ArrayBuffer, file: FileHandle, meta: Metadata) {
  const isPresent = meta[`${name}_present`]
  const length = meta[`${name}_length`]
  const position = meta[`${name}_offset`]

  if(!isPresent)
    return {status: 'err', reason: `Cannot read section '${name}' when not indicated in metadata.`}

  const result = await file.write(new Uint8Array(buffer), 0, length, position)
  if(result.bytesWritten != length)
    return {status: 'err', reason: `Failed to read section '${name}'. Expected to write ${length} bytes, got ${result.bytesWritten}`}

  return {status: 'ok'}
}

export function unpack(x: number): Z {
  return [x & 255, (x >> 8) & 255]
}

export function group(xs) {
  let buf = []
  for(let i=0; i<xs.length; i+=2) {
    buf.push([xs[i],xs[i+1]])
  }
  return buf
}

export function formatZ([r, i]: Z) {
  return i >= 0 ? `${r}+${i}i`
                : `${r}${i}i`
}

/*
 *  Result monad
 */

export function ok<T>(value: T = null): Result<T> {
  return {status: 'ok', value}
}

export function is_ok<T>(result: Result<T>): boolean {
  return result.status == 'ok'
}

export function fail<T>(reason: string): Result<T> {
  return {status: 'err', reason, location: [], value: null}
}

/** Fail at a specified location. */
export function fail_at<T>(reason: string, place: ErrorLocation): Result<T> {
  return {status: 'err', reason, location: [place], value: null}
}

/** Convert a failure with another success type, without adding a bread crumb to the trail. */
export function fail_with<T, U>(result: Result<T>): Result<U> {
  return {status: 'err', reason: result.reason, location: result.location, value: null}
}

/** Convert a failure with another success type, adding a bread crumb to the trail. */
export function fail_from<T, U>(result: Result<T>, place: ErrorLocation): Result<U> {
  return {status: 'err', reason: result.reason, location: [place, ...result.location], value: null}
}

export function fmap<A, B>(f: (x:A) => B, ma: Result<A>): Result<B> {
  if(!is_ok(ma))
    return fail_with(ma)
  else
    return ok(f(ma.value))
}

/** Pipe a result into a resultant function. */
export function bind<A, B>(ra: Result<A>, f: Resultant<A,B>): Result<B> {
  if(!is_ok(ra))
    return fail_with(ra)
  else
    return f(ra.value)
}

/** Pipe a promised result into a resultant function. */
export async function async_bind<A, B>(pa: Promise<Result<A>>, f: Resultant<A,B>): Promise<Result<B>> {
  return bind(await pa, f)
}

/** Kleisli composition of two resultants. */
export function arrow<A,B,C>(f: Resultant<A,B>, g: Resultant<B,C>): Resultant<A,C> {
  return x => bind(f(x), g)
}

/** Kleisli composition of an async resultant to a resultant. */
export function async_arrow<A,B,C>(f: AsyncResultant<A,B>, g: Resultant<B,C>): AsyncResultant<A,C> {
  return async x => bind(await f(x), g)
}

/** Get the results for a list of action Promises. */
export async function await_all<T>(tasks: Promise<Result<T>>[]): Promise<Result<T[]>> {
  const values = []
  for(let task of tasks) {
    const result = await task
    if(result.status != 'ok')
      return fail(result.reason)
    values.push(result.value)
  }
  return ok(values)
}

export function all<T>(results: Result<T>[]): Result<T[]> {
  const vals = []
  for(let i=0; i<results.length; i++) {
    const result = results[i]
    if(!is_ok(result)) 
      return fail_from(result, i)
    vals.push(result.value)
  }
  return ok(vals)
}


/*
 *  Data access (should go somewhere else)
 */

/** Get the head or tail margin samples for a given source ID. */
export function get_margin(id: number, data: Int8Array, meta: Metadata, getHead=true, includeOverlap=true) {
  const lineSz = meta.margin_samples * 2
  const offset = getHead ? 0 : lineSz
  const position = id * lineSz * 2 + (getHead ? 0 : lineSz) + (!getHead && !includeOverlap ? lineSz/2 : 0)
  const length = includeOverlap ? lineSz : lineSz/2
  return data.subarray(position, position + length)
}

/** Get a whole block from the data section. */
export function get_block(id: number, data: Int8Array, meta: Metadata) {
  const sz = meta.samples_per_line * meta.num_sources * 2
  return data.subarray(id * sz, (id+1) * sz)
}

/** Get a single line from a block. */
export function get_line(id: number, blockData: Int8Array, meta: Metadata) {
  return blockData.subarray(id * meta.samples_per_line * 2, (id+1) * meta.samples_per_line * 2)
}


/*
 *  Complex numbers
 */

export function complex_mul([a, b]:Z, [c, d]:Z): Z {
  return [a*c - b*d, a*d + b*c]
}

export function complex_rotate(radians: number, x: Z): Z {
  return complex_mul(x, [Math.cos(radians), -Math.sin(radians)]) 
}


/*
 *  File IO
 */

/** Read the entire contents of a file into an ArrayBuffer. */
export async function read_file(path: string): Promise<Result<ArrayBuffer>> {
  return await fs.readFile(path)
    .then(buffer => ok(buffer.buffer))
    .catch(reason => fail(reason))
}


/*
 *  Higher-order functions
 */

/** Apply a list of functions to a list of values pair-wise. */
export function apply_each<A, B>(xs: A[], fs: ((x:A) => B)[]): B[] {
  return fs.map((f,i) => f(xs[i]))
}


/*
 *  String formatting
 */

export function format_colour(colour: 'yellow' | 'cyan', x: string) {
  const code = colour == 'yellow' ? '\x1b[33m' : '\x1b[36m'
  return `${code}${x}\x1b[0m`
}

/** Convert an integer to a right-aligned string.
 * No padding is performed if the integer converts to a string longer than `width`. 
 */
export function align_int(width: number, x: number): string {
  return x.toString().padStart(width, ' ')
}

/** Convert a float to a right-aligned string with the given precision.
 * No padding is performed if the integer converts to a string longer than `width`. 
 */
 export function align_float(width: number, digits: number, x: number): string {
  return x.toFixed(digits).padStart(width, ' ')
}
