import * as fs from 'node:fs/promises'
import { FileHandle } from 'fs/promises'
import * as dt from './dt.js'
import { initMetadata, read_header } from './util.js'
import type { Metadata, OutputDescriptor, SectionDescriptor, RepointDescriptor } from './types'

/** Load a subfile, gather basic info. */
export async function load_subfile(filename) {
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
