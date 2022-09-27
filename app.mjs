import * as fs from 'node:fs/promises'
import { parseCommandLine } from './cli.mjs'
import * as dt from './dt.mjs'
import * as layout from './layout.mjs'
import { read_block, read_section } from './util.mjs'

// HDR_SIZE            4096                 POPULATED           1                    OBS_ID              1343457784           SUBOBS_ID           1343457864
// MODE                NO_CAPTURE           UTC_START           2022-08-02-06:42:46  OBS_OFFSET          80                   NBIT                8
// NPOL                2                    NTIMESAMPLES        64000                NINPUTS             272                  NINPUTS_XGPU        288
// APPLY_PATH_WEIGHTS  0                    APPLY_PATH_DELAYS   0                    INT_TIME_MSEC       2000                 FSCRUNCH_FACTOR     200
// APPLY_VIS_WEIGHTS   0                    TRANSFER_SIZE       5605376000           PROJ_ID             C001                 EXPOSURE_SECS       8
// COARSE_CHANNEL      129                  CORR_COARSE_CHANNEL 24                   SECS_PER_SUBOBS     8                    UNIXTIME            1659422566
// UNIXTIME_MSEC       0                    FINE_CHAN_WIDTH_HZ  40000                NFINE_CHAN          32                   BANDWIDTH_HZ        1280000
// SAMPLE_RATE         1280000              MC_IP               0.0.0.0              MC_PORT             0                    MC_SRC_IP           0.0.0.0
const BLOCKS_PER_SUB = 160
const FFT_PER_BLOCK = 10
const POINTINGS_PER_SUB = BLOCKS_PER_SUB * FFT_PER_BLOCK
const SAMPLES_PER_SEC = 1280000
const SAMPLES_PER_LINE = (SAMPLES_PER_SEC * 8) / BLOCKS_PER_SUB
const SUB_LINE_SIZE = (SAMPLES_PER_SEC * 8 * 2) / BLOCKS_PER_SUB // Sub line size in bytes


/** Parse a header fragment, returning a list of key,value pairs. */
function parse_header(buf) {
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

function print_header(header, headerBuf, opts) {
  if(opts.format_out == 'pretty') {
    const hdr = Object.entries(header).map(([k,v]) => [k.padEnd(19, ' '), v.toString().padEnd(16, ' ')])
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




async function main(args) {
  const parseResult = parseCommandLine(args)
  if(parseResult.status != 'ok') {
    console.error(parseResult.reason)
    return
  }
  switch(parseResult.command) {
  case 'show':
    return runShow(parseResult.fixedArgs[0], parseResult.opts)
  case 'dt':
    return runDt(parseResult.fixedArgs[0], parseResult.opts)
  case 'info':
    return runInfo(parseResult.fixedArgs[0], parseResult.opts)
  case 'dump':
    return runDump(parseResult.fixedArgs[0], parseResult.fixedArgs[1], parseResult.opts)
  case 'repoint':
    return runRepoint(parseResult.fixedArgs[0], parseResult.fixedArgs[1], parseResult.opts)
  case null:
      return
  }
}

async function runShow(filename, opts) {
  const file = await fs.open(filename, 'r')
  const meta = initMetadata()


  // Read header
  // We always do this, even if we're not printing it, since everything else depends on it (except
  // when only printing the header in binary, but we're not optimising that special case).
  const headerBuf = new ArrayBuffer(4096)
  let result = await file.read(new Uint8Array(headerBuf), {length: 4096}  )
  if(result.bytesRead != 4096) throw `Failed to read header data. Expected to read 4096 bytes, got ${result.bytesRead}`
  const header = parse_header(headerBuf)
  meta.num_sources = header.NINPUTS
  meta.num_frac_delays = POINTINGS_PER_SUB
  if(opts.show_header)
    print_header(header, headerBuf, opts)

  if(!(opts.show_delay_table || opts.show_data))
    return {status: 'ok'}

  // Read tile metadata
  const delayTableLength = 20 + 2 * POINTINGS_PER_SUB
  const delayTablePad = 0 // 4 - delayTableLength % 4
  const delayTableSectionLength = (delayTableLength + delayTablePad) * header.NINPUTS
  const delayTableBuf = new ArrayBuffer(delayTableSectionLength)
  result = await file.read(new Uint8Array(delayTableBuf), {length: delayTableSectionLength})
  if(result.bytesRead != delayTableSectionLength)
    throw `Failed to read header data. Expected to read ${delayTableSectionLength} bytes, got ${result.bytesRead}`
  const tiles = []
  for(let i=0; i<header.NINPUTS; i++) {
    const view = new DataView(delayTableBuf, i*(delayTableLength+delayTablePad), delayTableLength)
    const tile = dt.parse_delay_table_row(view, meta)
    tiles.push(tile)
  }
  if(opts.show_delay_table)
    dt.print_delay_table(tiles, delayTableBuf, opts)

  // Read voltages
  if(!opts.show_data) {
    await file.close()
    return
  }
  const selected_ids = []
  if(opts.selected_sources != null) {
    for(let src of opts.selected_sources) {
      const idx = tiles.findIndex(tile => tile.rf_input == src)
      if(idx == -1) {
        console.warn(`Warning: source ${src} not found in file.`)
        continue
      }
      selected_ids.push(idx)
    }
  } else {
    for(let i=0; i<tiles.length; i++)
      selected_ids.push(i)
  }
  const blockLength = meta.num_sources * SUB_LINE_SIZE
  const block1Buf = new ArrayBuffer(blockLength)
  const block1 = new Int8Array(block1Buf)
  result = await file.read(new Uint8Array(block1Buf), {length: blockLength, position: 4096 + blockLength})

  if(result.bytesRead != blockLength)
    return {status: 'err', reason: `Failed to read sample data. Expected to read ${blockLength} bytes, got ${result.bytesRead}`}

  for(let i of selected_ids) {
    const xs = block1.subarray(i*SUB_LINE_SIZE, (i+1)*SUB_LINE_SIZE)
    let str = tiles[i].rf_input.toString().padStart(4, ' ') + '  '
    for(let j=0; j<opts.num_samples; j++) {
      let [re, im] = [xs[j*2], xs[j*2+1]]
      if(im >= 0) im = `+${im}`
      str = str + `${re}${im}i `.padStart(8, ' ')
    }
    console.log(str)
  }

  await file.close()
}

async function runDt(filename, opts) {
  const meta = initMetadata()
  meta.filename = filename
  meta.num_sources = opts.num_sources_in
  meta.num_frac_delays = opts.num_frac_delays_in
  const loadResult = await dt.load_delay_table(filename, opts, meta)
  if(loadResult.status != 'ok') {
    console.error(loadResult.reason)
    return
  }
  const table = loadResult.table
  if(opts.compare_file) {
    const loadCmpResult = await dt.load_delay_table(opts.compare_file, opts, meta)
    if(loadCmpResult.status != 'ok') {
      console.error(loadCmpResult.reason)
      return
    }
    const tableCmp = loadCmpResult.table
    const diffResult = dt.compare_delays(tableCmp, table)
    if(diffResult.status != 'ok') {
      console.log(diffResult.reason)
      return
    }
    dt.print_delay_table(diffResult.table, null, opts)
  } else {
    dt.print_delay_table(table, loadResult.binaryBuffer, opts)
  }
}

async function runInfo(filename, opts) {
  const loadResult = await load_subfile(filename)
  if(loadResult.status != 'ok') {
    console.error(loadResult.reason)
    return
  }
  const meta = loadResult.meta
  Object.entries(meta).forEach(([k, v]) => {
    console.log(`${k}: ${v}`)
  })
}

async function runDump(subfilename, outfilename, opts) {
  const loadResult = await load_subfile(subfilename)
  if(loadResult.status != 'ok') {
    console.error(loadResult.reason)
    return
  }
  const {file, meta} = loadResult

  let result = null
  if(opts.dump_section == 'preamble') {
    result = await dump_preamble(outfilename, file, meta)
    return
  } else if(opts.dump_section)
    result = await read_section(opts.dump_section, file, meta)
  else if(Number.isInteger(opts.dump_block))
    result = await read_block(opts.dump_block, file, meta)
  else {
    console.error('Nothing to do.')
    return
  }
  if(result.status != 'ok') {
    console.error(result.reason)
    return
  }
  
  const buf = result.buf
  await fs.writeFile(outfilename, new Uint8Array(buf))
  console.warn(`Wrote ${buf.byteLength} bytes to ${outfilename}`)
}

async function dump_preamble(outfilename, file, meta) {
  const headerResult = await read_section('header', file, meta)
  const dtResult =     await read_section('dt', file, meta)
  const udpmapResult = await read_section('udpmap', file, meta)
  const marginResult = await read_section('margin', file, meta)
  if(headerResult.status != 'ok') return headerResult
  if(dtResult.status != 'ok') return dtResult
  if(udpmapResult.status != 'ok') return udpmapResult
  if(marginResult.status != 'ok') return marginResult

  const outputMeta = { ...meta, filename: outfilename}
  const outputDescriptor = {
    meta: outputMeta,
    repoint: null,
    sections: {
      header: { content: headerResult.buf, type: 'buffer' },
      dt: { content: dtResult.buf, type: 'buffer' },
      udpmap: { content: udpmapResult.buf, type: 'buffer' },
      margin: { content: marginResult.buf, type: 'buffer' },
    },
  }
  const result = await layout.write_subfile(outputDescriptor)
  if(result.status != 'ok') {
    console.error(result.reason)
    return
  }
  console.warn(`Wrote ${result.bytesWritten} bytes to ${outfilename}`)
}

async function runRepoint(infilename, outfilename, opts) {
  const loadResult = await load_subfile(infilename)
  if(loadResult.status != 'ok') {
    console.error(loadResult.reason)
    return
  }
  const {file, meta} = loadResult

  const headerResult = await read_section('header', file, meta)
  const dtResult =     await read_section('dt', file, meta)
  const udpmapResult = await read_section('udpmap', file, meta)
  const marginResult = await read_section('margin', file, meta)
  if(headerResult.status != 'ok') return headerResult
  if(dtResult.status != 'ok') return dtResult
  if(udpmapResult.status != 'ok') return udpmapResult
  if(marginResult.status != 'ok') return marginResult

  const dtMeta = initMetadata()
  dtMeta.filename = opts.delay_table_filename
  const loadDtResult = await dt.load_delay_table(opts.delay_table_filename, {format_in: 'auto'}, dtMeta)
  if(loadDtResult.status != 'ok') {
    console.error(loadDtResult.reason)
    return
  }
  const origDtResult = dt.parse_delay_table_binary(dtResult.buf, {}, meta)
  if(origDtResult.status != 'ok') {
    console.error(origDtResult.reason)
    return
  }
  const origDt = origDtResult.table
  const newDt = loadDtResult.table
  const newDtBin = dt.serialise_delay_table(newDt, meta.num_sources, meta.num_frac_delays)

  const outputMeta = { ...meta, filename: outfilename}
  const outputDescriptor = {
    meta: outputMeta,
    repoint: {
      from: origDt,
      to: newDt,
      margin: new Uint16Array(marginResult.buf),
    },
    sections: {
      header: { content: headerResult.buf, type: 'buffer' },
      dt: { content: newDtBin, type: 'buffer' },
      udpmap: { content: udpmapResult.buf, type: 'buffer' },
      margin: { content: marginResult.buf, type: 'buffer' },
      data: { file, type: 'file' },
    },
  }
  const result = await layout.write_subfile(outputDescriptor)
  //const {file, meta} = loadResult

}

/** Read the header section from a subfile. */
async function read_header(file, meta) {
  const sectionResult = await read_section('header', file, meta)
  if(sectionResult.status != 'ok')
    return sectionResult

  const header = parse_header(sectionResult.buf)
  return {status: 'ok', header}
}

/** Read the delay table section from a subfile. */
async function read_delay_table(file, meta) {
  const sectionResult = await read_section('dt', file, meta)
  if(sectionResult.status != 'ok')
    return sectionResult

  const table = dt.parse_delay_table_binary(sectionResult.buf)
  return {status: 'ok', table}
}

/** Read the margin section from a subfile. */
async function read_margin(file, meta) {
  const sectionResult = await read_section('margin', file, meta)
  if(sectionResult.status != 'ok')
    return sectionResult

  return {status: 'ok', buf: sectionResult.buf}
}

/** Load a subfile, gather basic info. */
async function load_subfile(filename) {
  const file = await fs.open(filename, 'r')

  const meta = initMetadata()
  meta.filename = filename
  meta.filetype = 'subfile'
  meta.header_present = true
  meta.header_offset = 0
  meta.header_length = 4096
  meta.dt_present = true
  meta.dt_offset = 4096

  // TODO: these shouldn't have to be constants:
  meta.fft_per_block = 10
  meta.margin_packets = 2
  meta.samples_per_packet = 2048
  
  const headerResult = await read_header(file, meta)
  if(headerResult.status != 'ok')
    return headerResult
  const header = headerResult.header
  
  meta.num_sources = header.NINPUTS
  meta.sample_rate = header.SAMPLE_RATE
  meta.secs_per_subobs = header.SECS_PER_SUBOBS
  meta.observation_id = header.OBS_ID
  meta.subobservation_id = header.SUBOBS_ID
  
  meta.samples_per_line = header.NTIMESAMPLES 
  meta.blocks_per_sub = meta.sample_rate * meta.secs_per_subobs / meta.samples_per_line
  meta.sub_line_size = meta.samples_per_line * 2
  meta.num_frac_delays = meta.blocks_per_sub * meta.fft_per_block
  meta.udp_per_rf_per_sub = meta.sample_rate * meta.secs_per_subobs / meta.samples_per_packet
  meta.udp_payload_length = meta.samples_per_packet * 2
  meta.margin_samples = meta.margin_packets * meta.samples_per_packet
  meta.dt_length = meta.num_sources * (20 + meta.num_frac_delays*2)
  meta.block_length = meta.sub_line_size * meta.num_sources
  meta.data_present = true
  meta.data_offset = meta.header_length + meta.block_length
  meta.data_length = meta.block_length * meta.blocks_per_sub
  meta.udpmap_present = true
  meta.udpmap_offset = meta.dt_offset + meta.dt_length
  meta.udpmap_length = meta.num_sources * meta.udp_per_rf_per_sub / 8
  meta.margin_present = true
  meta.margin_offset = meta.udpmap_offset + meta.udpmap_length
  meta.margin_length = meta.num_sources * meta.margin_samples * 2 * 2

  return {status: 'ok', file, meta, header}
}



/** Create a new metadata object, used for tracking information about files. */
const initMetadata = () => ({
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
})

main(process.argv.slice(2)) /*.catch(e => {
  console.error(`ERROR: ${e}`)
})*/
