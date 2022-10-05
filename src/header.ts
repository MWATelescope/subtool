import {Metadata} from './types.js'
import { read_section } from './util.js'


// HDR_SIZE            4096                 POPULATED           1                    OBS_ID              1343457784           SUBOBS_ID           1343457864
// MODE                NO_CAPTURE           UTC_START           2022-08-02-06:42:46  OBS_OFFSET          80                   NBIT                8
// NPOL                2                    NTIMESAMPLES        64000                NINPUTS             272                  NINPUTS_XGPU        288
// APPLY_PATH_WEIGHTS  0                    APPLY_PATH_DELAYS   0                    INT_TIME_MSEC       2000                 FSCRUNCH_FACTOR     200
// APPLY_VIS_WEIGHTS   0                    TRANSFER_SIZE       5605376000           PROJ_ID             C001                 EXPOSURE_SECS       8
// COARSE_CHANNEL      129                  CORR_COARSE_CHANNEL 24                   SECS_PER_SUBOBS     8                    UNIXTIME            1659422566
// UNIXTIME_MSEC       0                    FINE_CHAN_WIDTH_HZ  40000                NFINE_CHAN          32                   BANDWIDTH_HZ        1280000
// SAMPLE_RATE         1280000              MC_IP               0.0.0.0              MC_PORT             0                    MC_SRC_IP           0.0.0.0

const HEADER_FIELDS = {
  HDR_SIZE:            { index: 0, type: 'number' },
  POPULATED:           { index: 1, type: 'number' },
  OBS_ID:              { index: 2, type: 'number' },
  SUBOBS_ID:           { index: 3, type: 'number' },
  MODE:                { index: 4, type: 'string' },
  UTC_START:           { index: 5, type: 'string' },
  OBS_OFFSET:          { index: 6, type: 'number' },
  NBIT:                { index: 7, type: 'number' },
  NPOL:                { index: 8, type: 'number' },
  NTIMESAMPLES:        { index: 9, type: 'number' },
  NINPUTS:             { index: 10, type: 'number' },
  NINPUTS_XGPU:        { index: 11, type: 'number' },
  APPLY_PATH_WEIGHTS:  { index: 12, type: 'number' },
  APPLY_PATH_DELAYS:   { index: 13, type: 'number' },
  INT_TIME_MSEC:       { index: 14, type: 'number' },
  FSCRUNCH_FACTOR:     { index: 15, type: 'number' },
  APPLY_VIS_WEIGHTS:   { index: 16, type: 'number' },
  TRANSFER_SIZE:       { index: 17, type: 'number' },
  PROJ_ID:             { index: 18, type: 'string' },
  EXPOSURE_SECS:       { index: 19, type: 'number' },
  COARSE_CHANNEL:      { index: 20, type: 'number' },
  CORR_COARSE_CHANNEL: { index: 21, type: 'number' },
  SECS_PER_SUBOBS:     { index: 22, type: 'number' },
  UNIXTIME:            { index: 23, type: 'number' },
  UNIXTIME_MSEC:       { index: 24, type: 'number' },
  FINE_CHAN_WIDTH_HZ:  { index: 25, type: 'number' },
  NFINE_CHAN:          { index: 26, type: 'number' },
  BANDWIDTH_HZ:        { index: 27, type: 'number' },
  SAMPLE_RATE:         { index: 28, type: 'number' },
  MC_IP:               { index: 29, type: 'string' },
  MC_PORT:             { index: 30, type: 'number' },
  MC_SRC_IP:           { index: 31, type: 'string' },
  MWAX_U2S_VER:        { index: 32, type: 'string' },
}

export function print_header(header, headerBuf: ArrayBuffer, opts) {
  if(opts.format_out == 'pretty') {
    const hdr = Object.entries(header).map(([k,v]) => [k.padEnd(19, ' '), v.toString().padEnd(20, ' ')])
    for(let i=0; i<hdr.length; i+=4) {
      const rowItems = hdr.slice(i, i+4)
      const row = rowItems.reduce((acc,[k,v]) => `${acc}${k} ${v}`, '')
      console.log(row)
    }
  } else if(opts.format_out == 'csv') {
    Object.entries(header).forEach(([k,v]) => console.log(`${k},${v}`))
  } else if(opts.format_out == 'bin') {
    process.stdout.write(Buffer.from(headerBuf))
  } else {
    throw `Unsupported output format for header: ${opts.format_out}`
  }
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
    if(k in HEADER_FIELDS && HEADER_FIELDS[k].type == 'number')
      return [k, Number.parseInt(v)]
    else
      return [k, v]
  })
  return Object.fromEntries(fields)
}

/** Read the header section from a subfile. */
export async function read_header(file, meta) {
  const sectionResult = await read_section('header', file, meta)
  if(sectionResult.status != 'ok')
    return sectionResult

  const header = parse_header(sectionResult.buf)
  return {status: 'ok', header}
}

export function set_header_value(key, value, header, force=false) {
  if(key in HEADER_FIELDS || force) {
    switch(HEADER_FIELDS[key]?.type) {
      case 'number':
        header[key] = Number(value)
        break
      case 'string':
        header[key] = value
        break
      default:
        header[key] = value
        console.warn(`Warning: unknown type for header field ${key}.`)
    }
    return {status: 'ok'}
  } else return {status: 'invalid', reason: `No such key: ${key}.`}
}

export function serialise_header(header, meta: Metadata): ArrayBuffer {
  return (new TextEncoder()).encode(Object.entries(header)
    .map(([k,v]) => [k, v, k in HEADER_FIELDS ? HEADER_FIELDS[k].index : 9999])
    .sort((a,b) => a[2] > b[2] ? 1 : a[2] < b[2] ? -1 : 0)
    .map(([k,v,_]) => `${k} ${v}`)
    .join('\n')
    .concat('\n')
    .padEnd(meta.header_length, '\0')).buffer
}

