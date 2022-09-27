/** Subfile binary format.
 * 
 * OutputDescriptor
 * A data structure describing a subfile to be written, including any delay-
 * shifting to be applied. The top-level keys of an OutputDescriptor are:
 * 
 *   meta      Associated metadata object (required).
 *   repoint   Delay-shift descriptor (optional).
 *   sections  Section descriptors (required).
 * 
 * The metadata object is described elsewhere. The two other descriptors are
 * as follows:
 * 
 * RepointDescriptor
 * A data structure describing delay-adjustments to be applied.
 * 
 *   from      Delay table corresponding to existing data.
 *   to        New delay table after repointing.
 *   margin    ArrayBuffer of margin section.
 * 
 * SectionDescriptor
 * Describes the content with which to populate each section in the subfile.
 * 
 *   content   Reference to content data.
 *   type      Content type:
 *               buffer    An ArrayBuffer to be copied in directly.
 *               object    Object representation to be serialised.
 *               subfile   Source subfile from which to extract data.
 * 
 * Example:
 * 
 *     const exampleOutputDescriptor = {
 *       meta: metadataObject,
 *       repoint: {
 *         from: delayTableObj,
 *         to: delayTableObj,
 *         margin: marginBuf,
 *       },
 *       sections: {
 *         header: {
 *           content: headerBuf,
 *           type: "buffer",
 *         },
 *         dt: {
 *           content: delayTableObj,
 *           type: "object",
 *         },
 *         udpmap: {
 *           content: udpmapBuf,
 *           type: "buffer",
 *         },
 *         margin: {
 *           content: marginBuf,
 *           type: "buffer",
 *         },
 *         data: {
 *           content: subfileHandle,
 *           type: "subfile",
 *         },
 *       }
 *     }
 * 
 */

import * as fs from 'node:fs/promises'
// @filename: types.ts
import type { Metadata, OutputDescriptor, SectionDescriptor, RepointDescriptor } from './types.js'
// @filename: common.ts
import { read_block } from './util.mjs'
import { FileHandle } from 'node:fs/promises'
// @filename: repoint.ts
import * as rp from './repoint.mjs'

/** Write out a subfile given an output descriptor. */
export async function write_subfile(output_descriptor: OutputDescriptor, opts) {
  const { meta, repoint, sections } = output_descriptor
  let bytesWritten: number = 0
  // Create a buffer to hold the preamble: header + block 0
  // This way we can use the exact offsets as given in the metadata.
  const preamble = Buffer.alloc(meta.header_length + meta.block_length)
  Buffer.from(sections.header.content).copy(preamble, meta.header_offset, 0, meta.header_length)
  Buffer.from(sections.dt.content).copy(preamble, meta.dt_offset, 0, meta.dt_length)
  Buffer.from(sections.udpmap.content).copy(preamble, meta.udpmap_offset, 0, meta.udpmap_length)
  Buffer.from(sections.margin.content).copy(preamble, meta.margin_offset, 0, meta.margin_length)

  const file = await fs.open(meta.filename, 'w')
  file.write(preamble)
  bytesWritten += preamble.byteLength
  
  if(sections.data) {
    const curDelays = repoint.from.map(row => row.ws_delay)//(Array(Number(meta.num_sources))).fill(0)
    const newDelays = repoint.to.map(row => row.ws_delay) //(Array(Number(meta.num_sources))).fill(0)
    //console.log(curDelays)
  
    let blockBuf:     ArrayBuffer = new ArrayBuffer(meta.block_length)
    let margin:       Uint16Array = repoint.margin
    let outBlock:     Uint16Array = new Uint16Array(blockBuf)
    let lastBlock:    Uint16Array | null = null
    let currentBlock: Uint16Array | null = null
    let nextBlock:    Uint16Array | null = null 
    
    let firstBlockResult = await read_block(1, sections.data.file, meta)
    if(firstBlockResult.status != 'ok')
      return firstBlockResult
  
    nextBlock = new Uint16Array(firstBlockResult.buf)
  
    for(let blockNum=1; blockNum<=meta.blocks_per_sub; blockNum++) {
      lastBlock = currentBlock
      currentBlock = nextBlock
  
      if(blockNum < meta.blocks_per_sub) {
        let nextBlockResult = await read_block(blockNum+1, sections.data.file, meta)
        if(nextBlockResult.status != 'ok')
          return nextBlockResult
        nextBlock = new Uint16Array(nextBlockResult.buf)
      }
  
      rp.timeShift(blockNum, currentBlock, lastBlock, nextBlock, outBlock, curDelays, newDelays, margin, meta, opts)
      //console.log(blockBuf)
      //throw "die"
      await file.write(Buffer.from(blockBuf))
      outBlock.fill(0)
      bytesWritten += blockBuf.byteLength
  
      console.log(`Wrote block ${blockNum}`)
    }
  }

  file.close()
  return { status: 'ok', bytesWritten }
}
