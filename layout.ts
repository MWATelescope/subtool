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
 *               remap     Array of pairs [A,B], replace A's data with B's, 
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
import { FileHandle } from 'node:fs/promises'
import type { Metadata, OutputDescriptor, SectionDescriptor, RepointDescriptor } from './types.js'
import { read_block } from './util.js'
import * as rp from './repoint.js'

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
  process.stderr.write('Writing blocks... ')
  if(sections.data) {
    if(sections.data.remap) {
    // To support remapping data streams, we introduce a layer of indirection.
    //const srcMap = meta.
    }
    
    if(repoint) {
      const repointResult: any = await rp.write_time_shifted_data(repoint.from, repoint.to, repoint.margin, sections.data.file, file, meta)
      if(repointResult.status != 'ok')
        return repointResult
      bytesWritten += repointResult.bytesWritten
    } else {  
     for(let blockNum=1; blockNum<=meta.blocks_per_sub; blockNum++) {
       const blockResult = await read_block(blockNum+1, sections.data.file, meta)
       if(blockResult.status != 'ok')
         return blockResult
       await file.write(Buffer.from(blockResult.buf))
       bytesWritten += blockResult.buf.byteLength
       process.stderr.write(`${blockNum} `)
      }
    }
    process.stderr.write('...done.\n')
  }

  file.close()
  return { status: 'ok', bytesWritten }
}

/** 
export async function apply_source_remapping(block, srcmap, file, meta) {

  return {status: 'ok'}
}



*/