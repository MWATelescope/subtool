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
import { FileHandle } from 'fs/promises'
import * as dt from './dt.js'
import { initMetadata, read_header, read_block } from './util.js'
import * as rp from './repoint.js'
import type { Metadata, OutputDescriptor, SourceMap } from './types'

/** Load a subfile, gather basic info. */
export async function load_subfile(filename: string) {
  const meta: Metadata = initMetadata()
  const file: FileHandle = await fs.open(filename, 'r')

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
  
  const headerResult: any = await read_header(file, meta)
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

  const dtResult: any = await dt.read_delay_table(file, meta)
  if(dtResult.status != 'ok')
    return dtResult
  const delayTable = dtResult.table
  
  meta.sources = delayTable.map(x => x.rf_input)

  return {status: 'ok', file, meta, header}
}



/** Write out a subfile given an output descriptor. */
export async function write_subfile(output_descriptor: OutputDescriptor, opts) {
  const { meta, repoint, remap, sections } = output_descriptor
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
    if(remap) {
      // To support remapping data streams, we introduce a layer of indirection.
      // Create a map of source IDs to index based on order of appearance.
      const srcMap: SourceMap = remap // Object.fromEntries(meta.sources.map((x, i) => [x, i]))
      const outBlockBuf = new ArrayBuffer(meta.block_length)
      const outputBlock = new Uint16Array(outBlockBuf)
      for(let blockNum=1; blockNum<=meta.blocks_per_sub; blockNum++) {
        const blockResult = await read_block(blockNum, sections.data.file, meta)
        if(blockResult.status != 'ok')
          return blockResult
        const inputBlock = new Uint16Array(blockResult.buf)
        copy_block_with_remapping(inputBlock, outputBlock, srcMap, meta.sources, meta)
        await file.write(Buffer.from(outBlockBuf))
        outputBlock.fill(0)
        bytesWritten += outBlockBuf.byteLength
        process.stderr.write(`${blockNum} `)
       }
    } else if(repoint) {
      const repointResult: any = await rp.write_time_shifted_data(repoint.from, repoint.to, repoint.margin, sections.data.file, file, meta)
      if(repointResult.status != 'ok')
        return repointResult
      bytesWritten += repointResult.bytesWritten
    } else {  
     for(let blockNum=1; blockNum<=meta.blocks_per_sub; blockNum++) {
       const blockResult = await read_block(blockNum, sections.data.file, meta)
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

function copy_block_with_remapping(inBlk: Uint16Array, outBlk: Uint16Array, map: SourceMap, outputSources: number[], meta: Metadata) {
  for(let outputLineIndex=0; outputLineIndex < outputSources.length; outputLineIndex++) {
    const source = outputSources[outputLineIndex]
    const inputLineIndex = map[source]
    const inputLine = rp.get_line(inputLineIndex, inBlk, meta)
    const outputLine = rp.get_line(outputLineIndex, outBlk, meta)
    outputLine.set(inputLine)
  }
}
