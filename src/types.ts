import { FileHandle } from "node:fs/promises";

export type Metadata = {
  filename: string;           // (ALL) path to file
  filetype: string;           // (ALL) "subfile" or "delay-table"
  observation_id: number;     // (sub) observation id
  subobservation_id: number;  // (sub) subobservation id
  num_sources: number;        // (ALL) number of sources that appear in the file
  num_frac_delays: number;    // (ALL) number of fractional delays used in the delay table
  sample_rate: number;        // (sub) sample rate for subobservation
  secs_per_subobs: number;    // (sub) length of subobservation in seconds
  samples_per_line: number;   // (sub) samples per line in each data block
  samples_per_packet: number; // (sub) number of samples in a udp packet
  udp_payload_length: number; // (sub) byte length of a udp packet payload
  udp_per_rf_per_sub: number; // (sub) number of packets per source in a subobservation
  sub_line_size: number;      // (sub) byte length of line in data block
  blocks_per_sub: number;     // (sub) number of blocks in the data section
  blocks_per_sec: number;     // (sub) number of blocks per second
  fft_per_block: number;      // (sub) number of fft sub-blocks per block
  block_length: number;       // (sub) byte length of 1 block
  margin_packets: number;     // (sub) number of margin packets per source at each end
  margin_samples: number;     // (sub) number of margin samples per source at each end
  dt_present: boolean;        // (ALL) is a delay table section present in the file?
  dt_offset: number;          // (ALL) byte offset of delay table
  dt_length: number;          // (ALL) byte length of delay table
  header_present: boolean;    // (ALL) is a header section present in the file?
  header_offset: number;      // (sub) byte offset of header
  header_length: number;      // (sub) byte length of header
  data_present: boolean;      // (ALL) is a data section present in the file?
  data_offset: number;        // (sub) byte offset of data section
  data_length: number;        // (sub) byte length of data section
  margin_present: boolean;    // (ALL) is a margin section present in the file?
  margin_offset: number;      // (sub) byte offset of margin section
  margin_length: number;      // (sub) byte length of margin section
  udpmap_present: boolean;    // (ALL) is a packet map section present in the file?
  udpmap_offset: number;      // (sub) byte offset of packet map
  udpmap_length: number;      // (sub) byte length of packet map
  sources: number[];          // (ALL) rf sources in order of appearance
}

export type OutputDescriptor = {
  meta: Metadata;
  repoint?: RepointDescriptor;
  remap?: SourceMap;
  resample?: ResampleDescriptor;
  sections: SectionDescriptorList;
}

export type ResampleDescriptor = {
  fns: TransformerSet;
  region: number;
}

export type RepointDescriptor = {
  from: any;
  to: any;
  margin: Uint16Array;
}

export type SectionDescriptor = {
  content?: ArrayBuffer;
  file?: FileHandle;
  type: string;
}

export type SectionDescriptorList = {
  header: SectionDescriptor;
  dt: SectionDescriptor;
  udpmap: SectionDescriptor;
  margin: SectionDescriptor;
  data?: SectionDescriptor;
}

export type DelayTableEntry = {
  rf_input: number;
  ws_delay: number;
  initial_delay: number;
  delta_delay: number;
  delta_delta_delay: number; 
  num_pointings: number;
  frac_delay: Int16Array;
}

export type SourceMap = {
  [k: number]: number;
}

/** A complex number. */
export type Z = [number, number];

/** Resampling transform function. */
export type TransformFn = (prev: Int8Array, cur: Z, next: Int8Array, time: number) => Z
export type TransformerSet =  { [index: number]: TransformFn }