import { FileHandle } from 'fs/promises'
import type { Metadata, Result, Z } from './types'

/** Create a new metadata object, used for tracking information about files. */
export const initMetadata = () => ({
  filename: null,          
  filetype: null,          
  observation_id: null,    
  subobservation_id: null, 
  num_sources: null,       
  num_frac_delays: null,   
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
})

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

/** Get the results for a list of action Promises. */
export async function await_all(tasks) {
  const values = []
  for(let task of tasks) {
    const result = await task
    if(result.status != 'ok') return result
    values.push(result.value)
  }
  return {status: 'ok', value: values}
}

export function all<T>(results: Result<T>[]): Result<T[]> {
  const vals = []
  for(let result of results) {
    if(result.status != 'ok') 
      return fail(result.reason)
    vals.push(result.value)
  }
  return ok(vals)
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

export function fail<T>(reason: string): Result<T> {
  return {status: 'err', reason, value: null}
}

export function ok<T>(value: T = null): Result<T> {
  return {status: 'ok', value}
}
