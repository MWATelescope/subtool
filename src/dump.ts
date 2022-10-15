import * as fs from 'node:fs/promises'
import { load_subfile, write_subfile, source_to_line } from './subfile.js'
import { fail, get_line, ok } from './util.js'
import type { Metadata, Result } from './types'
import { FileHandle } from 'node:fs/promises'
import {Cache, cache_create, print_cache_stats} from './cache.js'
import {read_block, read_margin_line, read_section} from './reader.js'

export async function runDump(subfilename: string, outfilename: string, opts) {
  const cache = cache_create(2 ** 30) // 1GB
  const loadResult = await load_subfile(subfilename)
  if(loadResult.status != 'ok') {
    console.error(loadResult.reason)
    return
  }
  const {file, meta} = loadResult

  let result: Result<ArrayBuffer> = null
  if(opts.dump_section == 'preamble') {
    result = await dump_preamble(outfilename, file, meta, cache)
    return
  } else if(opts.dump_section)
    result = await read_section(opts.dump_section, file, meta, cache)
  else if(Number.isInteger(opts.dump_block))
    result = await read_block(opts.dump_block, file, meta, cache)
  else if(Number.isInteger(opts.dump_source)) {
    result = await extract_source(opts.dump_source, opts.dump_with_margin, file, meta, cache)
  } else {
    console.error('Nothing to do.')
    return
  }
  if(result.status != 'ok') {
    console.error(result.reason)
    return
  }
  
  const buf = result.value
  await fs.writeFile(outfilename, new Uint8Array(buf))
  console.warn(`Wrote ${buf.byteLength} bytes to ${outfilename}`)
  print_cache_stats(cache)
  return {status: 'ok'}
}

/** Get the voltage sample data for a specified source ID.
 * 
 * Not implemented yet:
 * If the `includeMargin` option is set, all of the samples including the
 * margin data is extracted. If the delay table indicates the a whole-sample
 * delay has been applied, the margin samples are shifted so that the extracted
 * data represents a continuous stream with no repeated or omitted samples, but
 * the length of the stream is unaffected. This will result in N zero-valued
 * samples at the beginning or end of the extracted stream, where N is the
 * whole-sample delay. This is 
 */
async function extract_source(sourceId: number, includeMargin: boolean, file: FileHandle, meta: Metadata, cache: Cache): Promise<Result<ArrayBuffer>> {
  const getLineResult = await source_to_line(sourceId, file, meta, cache)
  if(getLineResult.status != 'ok')
    return fail(getLineResult.reason)
  const lineNum = getLineResult.value
  const bufSize = includeMargin ? meta.samples_per_line * meta.blocks_per_sub * 2 + meta.margin_samples*2 // 2 bytes each for half the margin samples per end * 2 ends
                                : meta.samples_per_line * meta.blocks_per_sub * 2
  const buf = new Int8Array(bufSize)
  if(includeMargin) {
    return fail('Extracting source with margin data is not implemented yet.')
    let result = await read_margin_line(lineNum, file, meta, cache, true)
    if(result.status != 'ok')
      return fail(result.reason)
  }
  let pos = includeMargin ? meta.margin_samples : 0
  for(let blockNum=1; blockNum<=meta.blocks_per_sub; blockNum++) {
    const result = await read_block(blockNum, file, meta, cache)
    if(result.status != 'ok')
      return result
    const block = new Int8Array(result.value)
    const line = get_line(lineNum, block, meta)
    buf.set(line, pos)
    pos += line.length
  }
  return ok(buf.buffer)
}

// ...this does not look right:
async function dump_preamble(outfilename: string, file: FileHandle, meta: Metadata, cache: Cache): Promise<Result<ArrayBuffer>> {
  /*const headerResult = await read_section('header', file, meta, cache)
  const dtResult =     await read_section('dt', file, meta, cache)
  const udpmapResult = await read_section('udpmap', file, meta, cache)
  const marginResult = await read_section('margin', file, meta, cache)
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
  const result = await write_subfile(outputDescriptor, null)
  if(result.status != 'ok') {
    console.error(result.reason)
    return
  }
  console.warn(`Wrote ${result.bytesWritten} bytes to ${outfilename}`)*/
  return fail('not implemented')
}
