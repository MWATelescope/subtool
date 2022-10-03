import { FileHandle } from 'fs/promises'
import type { Metadata } from './types'

/** Create a new metadata object, used for tracking information about files. */
export const initMetadata = () => ({
  filename: null,                  // (ALL) path to file
  filetype: null,                  // (ALL) "subfile" or "delay-table"
  observation_id: null,            // (sub) observation id
  subobservation_id: null,         // (sub) subobservation id
  num_sources: null,               // (ALL) number of sources that appear in the file
  num_frac_delays: null,           // (ALL) number of fractional delays used in the delay table
  sample_rate: null,               // (sub) sample rate for subobservation
  secs_per_subobs: null,           // (sub) length of subobservation in seconds
  samples_per_line: null,          // (sub) samples per line in each data block
  samples_per_packet: null,        // (sub) number of samples in a udp packet
  udp_payload_length: null,        // (sub) byte length of a udp packet payload
  udp_per_rf_per_sub: null,        // (sub) number of packets per source in a subobservation
  sub_line_size: null,             // (sub) byte length of line in data block
  blocks_per_sub: null,            // (sub) number of blocks in the data section
  fft_per_block: null,             // (sub) number of fft sub-blocks per block
  block_length: null,              // (sub) byte length of 1 block
  margin_packets: null,            // (sub) number of margin packets per source at each end
  margin_samples: null,            // (sub) number of margin samples per source at each end
  dt_present: null,                // (ALL) is a delay table section present in the file?
  dt_offset: null,                 // (ALL) byte offset of delay table
  dt_length: null,                 // (ALL) byte length of delay table
  header_present: null,            // (ALL) is a header section present in the file?
  header_offset: null,             // (sub) byte offset of header
  header_length: null,             // (sub) byte length of header
  data_present: null,              // (ALL) is a data section present in the file?
  data_offset: null,               // (sub) byte offset of data section
  data_length: null,               // (sub) byte length of data section
  margin_present: null,            // (ALL) is a margin section present in the file?
  margin_offset: null,             // (sub) byte offset of margin section
  margin_length: null,             // (sub) byte length of margin section
  udpmap_present: null,            // (ALL) is a packet map section present in the file?
  udpmap_offset: null,             // (sub) byte offset of packet map
  udpmap_length: null,             // (sub) byte length of packet map
  sources: null,                   // (ALL) rf sources in order of appearance
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

/** Read the header section from a subfile. */
export async function read_header(file, meta) {
  const sectionResult = await read_section('header', file, meta)
  if(sectionResult.status != 'ok')
    return sectionResult

  const header = parse_header(sectionResult.buf)
  return {status: 'ok', header}
}

/** Read the margin section from a subfile. */
export async function read_margin(file, meta) {
  const sectionResult = await read_section('margin', file, meta)
  if(sectionResult.status != 'ok')
    return sectionResult

  return {status: 'ok', buf: sectionResult.buf}
}


/** Parse a header fragment, returning a list of key,value pairs. */
export function parse_header(buf) {
  const INTEGER_FIELDS = [
    'HDR_SIZE', 'POPULATED', 'OBS_ID', 'SUBOBS_ID', 'OBS_OFFSET', 'NBIT', 'NPOL', 'NTIMESAMPLES', 'NINPUTS', 
    'NINPUTS_XGPU', 'APPLY_PATH_WEIGHTS', 'APPLY_PATH_DELAYS', 'INT_TIME_MSEC', 'FSCRUNCH_FACTOR', 'APPLY_VIS_WEIGHTS', 
    'TRANSFER_SIZE', 'EXPOSURE_SECS', 'COARSE_CHANNEL', 'CORR_COARSE_CHANNEL', 'SECS_PER_SUBOBS', 'UNIXTIME',
    'UNIXTIME_MSEC', 'FINE_CHAN_WIDTH_HZ', 'NFINE_CHAN', 'BANDWIDTH_HZ', 'SAMPLE_RATE', 'MC_PORT']
  const text = new TextDecoder().decode(buf).replace(/\0*/g,'').trim()
  const fields = text.split('\n').map(x => x.split(' ')).map(([k,v]) => {
    if(k in INTEGER_FIELDS)
      return [k, Number.parseInt(v)]
    else if(k == 'UTC_START')
      return [k, v.replace(/[-:]/g, '')]
    else
      return [k, v]
  })
  return Object.fromEntries(fields)
}