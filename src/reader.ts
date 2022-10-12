/** Caching subfile reader.
 * 
 * All "read" functions in this module return ArrayBuffers.
 */

import {FileHandle} from "node:fs/promises"
import {Cache, cache_add, cache_get} from "./cache.js"
import {parse_delay_table_binary} from "./dt.js"
import {Metadata, Result} from "./types"
import {fail, ok} from "./util.js"

const BLOCK_0_SECTIONS = ['dt', 'udpmap', 'margin']

/** Basic cached read operation. */
async function read(key: any, position: number, length: number, file: FileHandle, cache: Cache): Promise<Result<ArrayBuffer>> {
  let buf = cache_get(key, cache)
  if(buf == null) {
    buf = new ArrayBuffer(length)
    const result = await file.read(new Uint8Array(buf), 0, length, position)
    if(result.bytesRead != length)
      return fail(`Failed to read '${key}'. Expected to read ${length} bytes, got ${result.bytesRead}`)
    cache_add(key, buf, cache)
  }
  return ok(buf)
}

/** Read section data. */
export async function read_section(name: string, file: FileHandle, meta: Metadata, cache: Cache): Promise<Result<ArrayBuffer>> {
  const isPresent = meta[`${name}_present`]
  const length = meta[`${name}_length`]
  const position = meta[`${name}_offset`]
  if(!isPresent)
    return fail(`Cannot read section '${name}' when not indicated in metadata.`)
  return read(name, position, length, file, cache)
}

/** Read a whole block. */
export async function read_block(idx: number, file: FileHandle, meta: Metadata, cache: Cache): Promise<Result<ArrayBuffer>> {
  if(idx < 0 || idx > meta.blocks_per_sub)
    return fail(`Invalid block number ${idx}.`)
  const position = idx * meta.block_length + meta.header_length
  return read(`block-${idx}`, position, meta.block_length, file, cache)
}

/** Read a whole block, returning null if the block number is out of bounds or zero. */
export async function read_block_or_null(idx: number, file: FileHandle, meta: Metadata, cache: Cache): Promise<Result<ArrayBuffer>> {
  if(idx <= 0 || idx > meta.blocks_per_sub)
    return ok(null)
  return read_block(idx, file, meta, cache)
}

/** Read a source line from a block. */
export async function read_line(lineNum: number, blockNum: number, file: FileHandle, meta: Metadata, cache: Cache): Promise<Result<ArrayBuffer>> {
  if(lineNum < 0 || lineNum >= meta.num_sources)
    return fail(`Invalid source line number ${lineNum}.`)

  const blockResult: any = await read_block(blockNum, file, meta, cache)
  if(blockResult.status != 'ok')
    return blockResult
  
  const length = meta.sub_line_size
  const position = lineNum * meta.sub_line_size
  const buf = blockResult.buf.slice(position, position + length)

  return ok(buf)
}

/** Get the line number for a source ID. */
export async function read_delay_table(file: FileHandle, meta: Metadata, cache: Cache): Promise<Result<any>> {
  const readResult = await read_section('dt', file, meta, cache)
  if(readResult.status != 'ok')
    return fail(readResult.reason)
  const buf = readResult.value
  const parseResult: any = parse_delay_table_binary(buf, meta, 0)
  if(parseResult.status != 'ok')
    return fail(parseResult.reason)
  return ok(parseResult.table)
}

/** Get the line number for a source ID. */
export async function source_to_line(sourceId: number, file: FileHandle, meta: Metadata, cache: Cache): Promise<Result<number>> {
  const readResult = await read_delay_table(file, meta, cache)
  if(readResult.status != 'ok')
    return fail(readResult.reason)
  const table = readResult.value
  const idx = table.findIndex(row => row.rf_input == sourceId)
  if(idx == -1)
    return fail(`RF Source ID ${sourceId} not found in subfile.`)
  return ok(idx)
}

/** Read the head or tail margin samples for a given source ID. 
 * 
 * The `getHead` argument specifies whether to get the head or tail margin.
 */
export async function read_margin_line(lineNum: number, file: FileHandle, meta: Metadata, cache: Cache, getHead=true): Promise<Result<ArrayBuffer>> {
  if(lineNum < 0 || lineNum >= meta.num_sources)
    return fail(`Invalid source line number ${lineNum}.`)

  const marginResult: any = await read_section('margin', file, meta, cache)
  if(marginResult.status != 'ok')
    return marginResult

  const length = meta.margin_samples * 2
  const offset = getHead ? 0 : length
  const position = lineNum * length * 2 + offset
  const buf = marginResult.buf.slice(position, position + length)

  return ok(buf)
}
