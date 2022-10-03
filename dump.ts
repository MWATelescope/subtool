import * as fs from 'node:fs/promises'
import * as layout from './layout.mjs'
import { load_subfile } from './subfile.mjs'
import { read_block, read_section } from './util.mjs'

export async function runDump(subfilename, outfilename, opts) {
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
  else if(Number.isInteger(opts.dump_source)) {
    
  } else {
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
  const result = await layout.write_subfile(outputDescriptor, null)
  if(result.status != 'ok') {
    console.error(result.reason)
    return
  }
  console.warn(`Wrote ${result.bytesWritten} bytes to ${outfilename}`)
}
