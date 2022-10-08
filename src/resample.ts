import type { Metadata, OutputDescriptor, SectionDescriptor, RepointDescriptor, TransformFn, Z, TransformerSet, Result } from './types'
import { read_block, read_block_or_null } from './reader.js'
import {FileHandle} from 'fs/promises';
import {cache_create} from './cache.js';
import {await_all, fail, ok, read_margin} from './util.js';


export async function resample(fns: TransformerSet, region: number, infile: FileHandle, outfile: FileHandle, meta: Metadata) {
  const outBuf:   ArrayBuffer = new ArrayBuffer(meta.block_length)
  const outBlock: Int8Array = new Int8Array(outBuf)
  const cache = cache_create(2 ** 30) // 1GB
  let bytesWritten = 0;

  const marginResult: any = await read_margin(infile, meta)
  if(marginResult.status != 'ok') return marginResult
  const margin = new Int8Array(marginResult.buf)

  for(let blockNum=1; blockNum<=meta.blocks_per_sub; blockNum++) {
    const readResult = await await_all([
      read_block_or_null(blockNum-1, infile, meta, cache),
      read_block(blockNum, infile, meta, cache),
      read_block_or_null(blockNum+1, infile, meta, cache),
    ])
    if(readResult.status != 'ok') return readResult
    const [prevBlock, curBlock, nextBlock] = readResult.value.map(x => x ? new Int8Array(x) : null)
    
    resample_block(fns, region, blockNum, curBlock, prevBlock, nextBlock, outBlock, margin, meta)

    await outfile.write(Buffer.from(outBuf))
    outBlock.fill(0)
    bytesWritten += outBuf.byteLength
  
    process.stderr.write(`${blockNum} `)
  }
  //console.warn(`Cache stats: hits=${cache.stats.hits} misses=${cache.stats.misses} inserts=${cache.stats.inserts} flushes=${cache.stats.flushes} deletes=${cache.stats.deletes} retained=${cache.stats.retained} released=${cache.stats.released}`)
  return {status: 'ok', value: bytesWritten}
}

/** Write out a copy of a data block, transforming each sample with a given function.
 */
 export function resample_block(
    fns: TransformerSet,          // Transform function, with arrays of previous and next samples
    region: number,               // Number of samples on either side to supply transform function
    blockNum: number,             // Current block number
    srcBlock: Int8Array,          // Block containing samples to transform
    srcPrev: Int8Array | null,    // Previous block
    srcNext: Int8Array | null,    // Next block
    dstBlock: Int8Array,          // Output block
    marginData: Int8Array,        // Margin sample data
    meta: Metadata) {             // Metadata
  const blockTime = (blockNum-1) / meta.blocks_per_sec
  for(let rfSourceId=0; rfSourceId<meta.num_sources; rfSourceId++) {
    const dstLine = get_line(rfSourceId, dstBlock, meta)
    const curLine = get_line(rfSourceId, srcBlock, meta)

    if(!(rfSourceId in fns)) {
      dstLine.set(curLine)
      continue
    }

    const prevLine = srcPrev ? get_line(rfSourceId, srcPrev, meta)
                             : get_margin(rfSourceId, marginData, meta, true, false)
    const nextLine = srcNext ? get_line(rfSourceId, srcNext, meta)
                             : get_margin(rfSourceId, marginData, meta, false, false)
    const lineSz = meta.samples_per_line
    const fn = fns[rfSourceId]    

    for(let sampleIdx=0; sampleIdx<meta.samples_per_line; sampleIdx++) {
      const sample: Z = [curLine[sampleIdx*2], curLine[sampleIdx*2+1]]
      const sampleTime = blockTime + sampleIdx / meta.sample_rate

      let prevSamples = 
          region == 0         ? new Int8Array()
        : region <= sampleIdx ? curLine.subarray((sampleIdx - region)*2, sampleIdx*2)
        : sampleIdx == 0      ? prevLine.subarray(-region*2)
        : new Int8Array([...prevLine.subarray((sampleIdx - region)*2), 
                         ...curLine.subarray(0, sampleIdx*2)])
      
      let nextSamples = 
          region == 0                  ? new Int8Array()
        : region <= lineSz - sampleIdx ? curLine.subarray((sampleIdx  + 1)*2, (sampleIdx + region + 1)*2)
        : sampleIdx == lineSz - 1      ? nextLine.subarray(0, region*2)
        : new Int8Array([ ...curLine.subarray((sampleIdx + 1)*2),
                          ...nextLine.subarray(0, (region - (lineSz - sampleIdx - 1))*2)])
      
      const newSample = fn(prevSamples, sample, nextSamples, sampleTime)
      dstLine.set(newSample.map(Math.round), sampleIdx*2)
    }
  }
}
// TODO: also resample margin values!
// TODO: should these be aware of existing ws delays?
// TODO: left/right shift direction can change due to initial value with the opposite sign!
export function make_phase_gradient_resampler(samples_per_second: number, initial: number): TransformFn {
  return function phase_shift(prev: Int8Array, cur: Z, next: Int8Array, time: number): Z {
    const amount = initial + samples_per_second * time
    const ws = Math.trunc(amount)
    const frac = Math.abs(amount - ws)
    let [s1, s2] = [null, null]
    if(amount > 0) {
      s1 = ws == 0 ? cur : next.slice((ws-1)*2, (ws-1)*2 + 2)
      s2 = next.slice(ws*2, ws*2 + 2)
    } else if(amount < 0) {
      s1 = ws == 0 ? cur : prev.slice(prev.length + ws*2, prev.length + (ws+1)*2)
      s2 = prev.slice(prev.length + (ws-1)*2, prev.length + (ws)*2)
      //if(ws != 0 && time > 0) {
      //console.log(ws, frac, s1,s2)
      //throw 'asddf'
      //}
    } else {
      s1 = s2 = cur
    }
    const dir = [s2[0] - s1[0], s2[1] - s1[1]]
    //if(time == 3) {
    //  console.log(amount, ws, frac, s1, s2, dir)
    //  throw 'asdf'
    //}
    return [s1[0] + dir[0]*frac, s1[1] + dir[1]*frac]
  }
}

function make_scale_resampler(scale: number): TransformFn {
  return (prev: Int8Array, cur: Z, next: Int8Array, time: number) => [cur[0]*scale, cur[1]*scale]
}

const TRANSFORM_BUILDERS: {[k: string] : (...args) => TransformFn} = {
  scale: make_scale_resampler,
  linear: make_phase_gradient_resampler,
}

export function make_resampler_transform(name: string, args: number[]): Result<TransformFn> {
  if(!(name in TRANSFORM_BUILDERS))
    return fail(`Invalid transform function: ${name}`)
  return ok(TRANSFORM_BUILDERS[name](...args))
}

/** Get the head or tail margin samples for a given source ID. */
export function get_margin(id: number, data: Int8Array, meta: Metadata, getHead=true, includeOverlap=true) {
  const lineSz = meta.margin_samples * 2
  const offset = getHead ? 0 : lineSz
  const position = id * lineSz * 2 + (getHead ? 0 : lineSz) + (!getHead && !includeOverlap ? lineSz/2 : 0)
  const length = includeOverlap ? lineSz : lineSz/2
  return data.subarray(position, position + length)
}

/** Get a whole block from the data section. */
export function get_block(id: number, data: Int8Array, meta: Metadata) {
  const sz = meta.samples_per_line * meta.num_sources * 2
  return data.subarray(id * sz, (id+1) * sz)
}

/** Get a single line from a block. */
export function get_line(id: number, blockData: Int8Array, meta: Metadata) {
  return blockData.subarray(id * meta.samples_per_line * 2, (id+1) * meta.samples_per_line * 2)
}

///** Get the head or tail margin samples for a given source ID. */
//export function get_margin(id: number, data: Uint16Array, meta: Metadata, getHead=true) {
//  const sz = meta.margin_samples
//  const offset = getHead ? 0 : sz
//  return data.subarray(id*sz*2 + offset, id*sz*2 + offset + sz)
//}
//
///** Get a whole block from the data section. */
//export function get_block(id: number, data: Uint16Array, meta: Metadata) {
//  const sz = meta.samples_per_line * meta.num_sources
//  return data.subarray(id * sz, (id+1) * sz)
//}
//
///** Get a single line from a block. */
//export function get_line(id: number, blockData: Uint16Array, meta: Metadata) {
//  return blockData.subarray(id * meta.samples_per_line, (id+1) * meta.samples_per_line)
//}
//
///** Get a single line from a given block number in the data section. */ 
//export function get_line_in_block(lineId: number, blockId: number, data: Uint16Array, params) {
//  return get_line(lineId, get_block(blockId, data, params), params)
//}



//import {Metadata, Z} from "./types";
//
//
//
//function shift_phase_linear(a: Z, x: Z, v3: Z) {
//}
//
///** Convert a block-relative sample index to an absolute sample index. */
//function absolute_sample_index(rel: number, block: number, meta: Metadata): number {
//  return rel % meta.samples_per_line + Math.floor(rel / meta.samples_per_line)
//}
//
///** Convert an absolute sample index to a `[block, position]` relative index. */
//function relative_sample_index(idx: number, meta: Metadata): [number, number] {
//  return [
//    Math.floor(idx / meta.samples_per_line),
//    idx % meta.samples_per_line 
//  ]
//}
//
//function get_sample_range(from: number, to: number, meta: Metadata) {
//}
//

