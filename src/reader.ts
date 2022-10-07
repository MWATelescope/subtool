/** Caching subfile reader.
 * 
 * All "read" functions in this module return ArrayBuffers.
 */

import {FileHandle} from "node:fs/promises"
import {Cache, cache_add, cache_get} from "./cache.js"
import {Metadata} from "./types"

const BLOCK_0_SECTIONS = ['dt', 'udpmap', 'margin']

/** Basic cached read operation. */
async function read(key: any, position: number, length: number, file: FileHandle, cache: Cache) {
  let buf = cache_get(key, cache)
  if(buf == null) {
    buf = new ArrayBuffer(length)
    const result = await file.read(new Uint8Array(buf), 0, length, position)
    if(result.bytesRead != length)
      return {status: 'err', reason: `Failed to read '${key}'. Expected to read ${length} bytes, got ${result.bytesRead}`}
    cache_add(key, buf, cache)
  }
  return {status: 'ok', value: buf}
}

/** Read section data. */
export async function read_section(name: string, file: FileHandle, meta: Metadata, cache: Cache) {
  const isPresent = meta[`${name}_present`]
  const length = meta[`${name}_length`]
  const position = meta[`${name}_offset`]
  if(!isPresent)
    return {status: 'err', reason: `Cannot read section '${name}' when not indicated in metadata.`}
  return await read(name, position, length, file, cache)
}

/** Read a whole block. */
export async function read_block(idx: number, file: FileHandle, meta: Metadata, cache: Cache) {
  if(idx < 0 || idx > meta.blocks_per_sub)
    return {status: 'err', reason: `Invalid block number ${idx}.`}
  const position = idx * meta.block_length + meta.header_length
  return await read(`block-${idx}`, position, meta.block_length, file, cache)
}

/** Read a whole block, returning null if the block number is out of bounds or zero. */
export async function read_block_or_null(idx: number, file: FileHandle, meta: Metadata, cache: Cache) {
  if(idx <= 0 || idx > meta.blocks_per_sub)
    return {status: 'ok', value: null}
  return await read_block(idx, file, meta, cache)
}

/** Read a source line from a block. */
export async function read_line(lineNum: number, blockNum: number, file: FileHandle, meta: Metadata, cache: Cache) {
  if(lineNum < 0 || lineNum >= meta.num_sources)
    return {status: 'err', reason: `Invalid source line number ${lineNum}.`}

  const blockResult: any = await read_block(blockNum, file, meta, cache)
  if(blockResult.status != 'ok')
    return blockResult
  
  const length = meta.sub_line_size
  const position = lineNum * meta.sub_line_size
  const buf = blockResult.buf.slice(position, position + length)

  return {status: 'ok', value: buf }
}

/** Read the head or tail margin samples for a given source ID. 
 * 
 * The `getHead` argument specifies whether to get the head or tail margin.
 */
export async function read_margin_line(lineNum: number, file: FileHandle, meta: Metadata, cache: Cache, getHead=true) {
  if(lineNum < 0 || lineNum >= meta.num_sources)
    return {status: 'err', reason: `Invalid source line number ${lineNum}.`}

  const marginResult: any = await read_section('margin', file, meta, cache)
  if(marginResult.status != 'ok')
    return marginResult

  const length = meta.margin_samples * 2
  const offset = getHead ? 0 : length
  const position = lineNum * length * 2 + offset
  const buf = marginResult.buf.slice(position, position + length)

  return {status: 'ok', value: buf}
}