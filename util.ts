import { FileHandle } from 'fs/promises'
// @filename: types.ts
import type { Metadata, OutputDescriptor, SectionDescriptor, RepointDescriptor } from './types.js'

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
