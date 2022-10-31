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
import * as dtv2 from './dtv2.js'
import { read_header, serialise_header, set_header_value } from './header.js'
import { all, async_bind, await_all, fail, fail_with, init_metadata, is_ok, ok } from './util.js'
import * as rp from './repoint.js'
import * as rs from './resample.js'
import type { DelayTableV2, Metadata, OutputDescriptor, Result, SourceMap } from './types'
import type { Cache } from './cache'
import {cache_create} from './cache.js'
import {read_block, read_section} from './reader.js'


export type SubfileContext = {
  file: FileHandle,
  meta: Metadata,
  cache: Cache,
  header: any,
}

/** Load a subfile, gather basic info. */
export async function load_subfile(filename: string, mode='r', cache: Cache = null): Promise<Result<SubfileContext>> {
  if(cache == null)
    cache = cache_create(2 ** 30) // 1GB

  const meta: Metadata = init_metadata()
  const file: FileHandle = await fs.open(filename, mode)

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

  const headerResult: any = await read_header(file, meta, cache)
  if(headerResult.status != 'ok')
    return headerResult
  const header = headerResult.value

  meta.num_sources = header.NINPUTS
  meta.sample_rate = header.SAMPLE_RATE
  meta.secs_per_subobs = header.SECS_PER_SUBOBS
  meta.observation_id = header.OBS_ID
  meta.subobservation_id = header.SUBOBS_ID
  meta.samples_per_line = header.NTIMESAMPLES
  meta.margin_samples = header.MARGIN_SAMPLES ?? meta.margin_packets * meta.samples_per_packet
  meta.mwax_sub_version = header.MWAX_SUB_VER ?? 1
  
  meta.blocks_per_sub = meta.sample_rate * meta.secs_per_subobs / meta.samples_per_line
  meta.blocks_per_sec = meta.blocks_per_sub / meta.secs_per_subobs
  meta.sub_line_size = meta.samples_per_line * 2
  meta.num_frac_delays = meta.blocks_per_sub * meta.fft_per_block
  meta.udp_per_rf_per_sub = meta.sample_rate * meta.secs_per_subobs / meta.samples_per_packet
  meta.udp_payload_length = meta.samples_per_packet * 2
  meta.frac_delay_size = meta.mwax_sub_version == 1 ? 2 : 4

  meta.block_length = meta.sub_line_size * meta.num_sources
  meta.dt_entry_min_size = meta.mwax_sub_version == 1 ? 20 : 56
  meta.dt_length = meta.num_sources * (meta.num_frac_delays * meta.frac_delay_size + meta.dt_entry_min_size)
  meta.data_present = true
  meta.data_offset = meta.header_length + meta.block_length
  meta.data_length = meta.block_length * meta.blocks_per_sub
  meta.udpmap_present = true
  meta.udpmap_offset = meta.dt_offset + meta.dt_length
  meta.udpmap_length = meta.num_sources * meta.udp_per_rf_per_sub / 8
  meta.margin_present = true
  meta.margin_offset = meta.udpmap_offset + meta.udpmap_length
  meta.margin_length = meta.num_sources * meta.margin_samples * 2 * 2  
  
  const dtResult = await async_bind(
    read_section('dt', file, meta, cache), 
    buf => dtv2.parse_delay_table_binary(buf, meta.mwax_sub_version, meta.num_sources, meta.num_frac_delays)
  )
  if(!is_ok(dtResult))
    return fail_with(dtResult)
  meta.delay_table = dtResult.value
  meta.sources = meta.delay_table.entries.map(x => x.rf_input)

  return ok({file, meta, header, cache})
}



/** Write out a subfile given an output descriptor. */
export async function write_subfile(output_descriptor: OutputDescriptor, opts, cache: Cache) {
  const { meta, repoint, remap, resample, sections } = output_descriptor
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
        const blockResult = await read_block(blockNum, sections.data.file, meta, cache)
        if(blockResult.status != 'ok')
          return blockResult
        const inputBlock = new Uint16Array(blockResult.value)
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
    } else if(resample) {
      const resampleResult: any = await rs.resample(resample.fns, resample.region, sections.data.file, file, meta)
      if(resampleResult.status != 'ok')
        return resampleResult
      bytesWritten += resampleResult.value
    } else {  
     for(let blockNum=1; blockNum<=meta.blocks_per_sub; blockNum++) {
       const blockResult = await read_block(blockNum, sections.data.file, meta, cache)
       if(blockResult.status != 'ok')
         return blockResult
       await file.write(Buffer.from(blockResult.value))
       bytesWritten += blockResult.value.byteLength
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


/** Get the line number for a source ID. */
export async function source_to_line(sourceId: number, file: FileHandle, meta: Metadata, cache: Cache): Promise<Result<number>> {
  const idx = meta.delay_table.entries.findIndex(row => row.rf_input == sourceId)
  if(idx == -1)
    return fail(`RF Source ID ${sourceId} not found in subfile.`)
  return ok(idx)
}

/** Overwrite the samples for a source with a given line number. */
export async function overwrite_samples(lineNum: number, samples: Int8Array, meta: Metadata, file: FileHandle): Promise<Result<void>> {
  for(let blockNum=0; blockNum<meta.blocks_per_sub; blockNum++) {
    const position = blockNum * meta.sub_line_size
    const length = meta.sub_line_size
    const line = samples.subarray(position, position + length)
    await overwrite_line(lineNum, blockNum, line, meta, file)
  }
  return ok()
}

/** Overwrite the samples for a given line and *zero-indexed* data block ID. */
export async function overwrite_line(lineNum: number, blockNum: number, samples: Int8Array, meta: Metadata, file: FileHandle): Promise<Result<void>> {
  const position = meta.data_offset + blockNum * meta.block_length + lineNum * meta.sub_line_size
  const length = meta.sub_line_size
  await file.write(Buffer.from(samples), 0, length, position)
  return ok()
}

/** Overwrite the delay table. */
export async function overwrite_delay_table(table: DelayTableV2, meta: Metadata, file: FileHandle): Promise<Result<void>> {
  const result = dtv2.serialise_delay_table(table)
  if(!is_ok(result))
    return fail_with(result)
  const buf = result.value
  const position = meta.dt_offset
  const length = meta.dt_length
  await file.write(Buffer.from(buf), 0, length, position)
  return ok()
}

/** Overwrite an arbitrary section. */
export async function overwrite_section(name: string, buf: ArrayBuffer, meta: Metadata, file: FileHandle): Promise<Result<void>> {
  const isPresent = meta[`${name}_present`]
  const length = meta[`${name}_length`]
  const position = meta[`${name}_offset`]
  if(!isPresent)
    return fail(`Cannot write section '${name}' when not indicated in metadata.`)

  await file.write(Buffer.from(buf), 0, length, position)
  return ok()
}

/** Change the size of the delay table fractional delays from 2 to 4 bytes. */
export async function upgrade_delay_table(file: FileHandle, meta: Metadata, cache: Cache): Promise<Result<void>> {
  if(meta.mwax_sub_version == 2) {
    console.warn(`Nothing to do. This subfile is already in version 2 format.`)
    return ok()
  }
  const result = await await_all([
    read_section('udpmap', file, meta, cache),
    read_section('margin', file, meta, cache),
  ])
  const [udpmap, margin] = result.value
  meta.frac_delay_size = 4
  meta.dt_entry_min_size = 56
  const new_dt_length = meta.num_sources * (meta.dt_entry_min_size + meta.num_frac_delays*meta.frac_delay_size)
  const dt_length_diff = new_dt_length - meta.dt_length
  meta.dt_length = new_dt_length
  meta.udpmap_offset += dt_length_diff
  meta.margin_offset += dt_length_diff
  meta.delay_table.format_version = 2
  await overwrite_section('udpmap', udpmap, meta, file)
  await overwrite_section('margin', margin, meta, file)
  await overwrite_delay_table(meta.delay_table, meta, file)

  const headerResult = await read_header(file, meta, cache)
  if(headerResult.status != 'ok')
    return headerResult
  const header = headerResult.value
  const setResult = all([
    set_header_value('FRAC_DELAY_SIZE', 4, header, true),
    set_header_value('MWAX_SUB_VER', 2, header, true),
  ])
  if(setResult.status != 'ok')
    return fail(setResult.reason)
  const headerBuf = serialise_header(header, meta)
  const writeResult = await overwrite_section('header', headerBuf, meta, file)
  if(writeResult.status != 'ok')
    return writeResult
  
  return ok()
}
