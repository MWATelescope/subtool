// @filename: types.ts
import type { Metadata, OutputDescriptor, SectionDescriptor, RepointDescriptor } from './types.js'
// @filename: util.ts
import { read_block } from './util.mjs'

export async function write_time_shifted_data(from, to, margin, infile, outfile, meta) {
  let bytesWritten = 0;
  const curDelays = from.map(row => row.ws_delay)
  const newDelays = to.map(row => row.ws_delay)
  
  let blockBuf:     ArrayBuffer = new ArrayBuffer(meta.block_length)
  let outBlock:     Uint16Array = new Uint16Array(blockBuf)
  let lastBlock:    Uint16Array | null = null
  let currentBlock: Uint16Array | null = null
  let nextBlock:    Uint16Array | null = null 
  
  let firstBlockResult = await read_block(1, infile, meta)
  if(firstBlockResult.status != 'ok')
    return firstBlockResult
  
  nextBlock = new Uint16Array(firstBlockResult.buf)
  
  for(let blockNum=1; blockNum<=meta.blocks_per_sub; blockNum++) {
    lastBlock = currentBlock
    currentBlock = nextBlock
  
    if(blockNum < meta.blocks_per_sub) {
      let nextBlockResult = await read_block(blockNum+1, infile, meta)
      if(nextBlockResult.status != 'ok')
        return nextBlockResult
      nextBlock = new Uint16Array(nextBlockResult.buf)
    }
  
    time_shift(blockNum, currentBlock, lastBlock, nextBlock, outBlock, curDelays, newDelays, margin, meta)

    await outfile.write(Buffer.from(blockBuf))
    outBlock.fill(0)
    bytesWritten += blockBuf.byteLength
  
    process.stderr.write(`${blockNum} `)
  }
  return {status: 'ok', bytesWritten}
}

/** Write out a time-shifted copy of a data block.
 * 
 * ARGUMENTS
 * 
 *     src       Source data (Uint16Array)
 *     dst       Destination data (Uint16Array)
 *     current   List of currently applied whole-sample shifts in source order
 *     target    List of target (absolute) whole-sample shift values in source order
 *     margins   Margin data (Uint16Array)
 *     params    Config object defining various constants
 * 
 * 
 * DESCRIPTION
 * 
 * Shifts are computed relative to the existing shift offset, so a shift of +2 on data which is
 * already shifted backwards by -2 will produce unshifted output. The letters N and M refer to the
 * shift to be applied, and the existing level of shift respectively, where positive values denote
 * a shift forward, such values from the last block are copied into the next.
 * 
 * We write the output block-by-block and line-by-line. Each line comprises up to 2 of 3 possible
 * regions which are copied separately:
 * 
 *   1. The "body" is the region copied directly from the corresponding source block.
 *   2. The "head" is the shifted-forward region copied from the previous source block.
 *   3. The "tail" is the shifted-back region copied the following source block.
 * 
 * Head regions in the first block, and tail regions in the last block, are copied from the margin
 * data section of block 0, which contains, for each RF source, the first and last 4096 samples
 * received for it. When shifting forward by N samples, the source range for a block 1 head segment
 * is relative to the existing shift M, given by:
 * 
 *     range_start = 2048 - N - M
 *     range_end   = 2048 - M
 * 
 * When shifting back by N samples, the source range for a block 160 tail segment starts at an
 * index given by: (remember that a backwards shift implies negative values for N and M)
 * 
 *     range_start = 2048 - M
 *     range_end   = 2048 - N - M
 * 
 */
 export function time_shift(
    blockId: number,
    srcBlock: Uint16Array,
    srcPrev: Uint16Array | null,
    srcNext: Uint16Array | null,
    dstBlock: Uint16Array,
    current: number[], 
    target: number[], 
    marginData: Uint16Array, 
    meta: Metadata) {

  const nInputs = current.length
  //const {SAMPLES_PER_LINE, BLOCKS_PER_SUB, MARGIN_SAMPLES} = params
  const SAMPLES_PER_LINE = meta.samples_per_line
  const BLOCKS_PER_SUB = meta.blocks_per_sub
  const MARGIN_SAMPLES = meta.margin_samples
  const SUB_LINE_SIZE = meta.sub_line_size
  const blockLength = nInputs * SUB_LINE_SIZE
  const offsets = current.map((M, i) => [target[i] - M, M]) // N and M values for shift algorithm
  for(let rfSourceId=0; rfSourceId<nInputs; rfSourceId++) {
    const srcLine = srcBlock.subarray(rfSourceId * SAMPLES_PER_LINE, (rfSourceId+1) * SAMPLES_PER_LINE)
    const dstLine = dstBlock.subarray(rfSourceId * SAMPLES_PER_LINE, (rfSourceId+1) * SAMPLES_PER_LINE)
    const [N, M] = offsets[rfSourceId]
    const headLength = Math.max(0, N)
    const tailLength = Math.abs(Math.min(0, N))
    // Copy body
    const bodyLength = SAMPLES_PER_LINE - headLength - tailLength
    const srcBody = srcLine.subarray(tailLength, tailLength + bodyLength)
    const dstBody = dstLine.subarray(headLength, headLength + bodyLength)
    dstBody.set(srcBody)
    // Copy head
    if(headLength > 0) {
      const dstHead = dstLine.subarray(0, headLength)
      let srcHead: Uint16Array | null = null
      if(blockId > 1) {
        const prevLine = srcPrev.subarray(rfSourceId * SAMPLES_PER_LINE, (rfSourceId+1) * SAMPLES_PER_LINE)
        srcHead = prevLine.subarray(-headLength)
      } else {
        const headMargin = getMargin(rfSourceId, marginData, meta, true)
        srcHead = headMargin.subarray(MARGIN_SAMPLES/2 - N - M - 1, MARGIN_SAMPLES/2 - M - 1)
      }
      dstHead.set(srcHead)
    } else if(tailLength > 0) {
      const dstTail = dstLine.subarray(-tailLength)
      let srcTail: Uint16Array | null = null
      if(blockId < BLOCKS_PER_SUB - 1) {
        const nextLine = srcNext.subarray(rfSourceId * SAMPLES_PER_LINE, (rfSourceId+1) * SAMPLES_PER_LINE)
        srcTail = nextLine.subarray(0, tailLength)
      } else {
        const tailMargin = getMargin(rfSourceId, marginData, meta, false)
        srcTail = tailMargin.subarray(MARGIN_SAMPLES/2 - M + 1, MARGIN_SAMPLES/2 - N - M + 1)
      }
      dstTail.set(srcTail)
    }
  }
    
}

/** Get the head or tail margin samples for a given source ID. */
export function getMargin(id: number, data: Uint16Array, meta: Metadata, getHead=true) {
  const sz = meta.margin_samples
  const offset = getHead ? 0 : sz
  return data.subarray(id*sz*2 + offset, id*sz*2 + offset + sz)
}

/** Get a whole block from the data section. */
export function getBlock(id: number, data: Uint16Array, meta: Metadata) {
  const sz = meta.samples_per_line * meta.num_sources
  return data.subarray(id * sz, (id+1) * sz)
}

/** Get a single line from a block. */
export function getLine(id: number, blockData: Uint16Array, meta: Metadata) {
  return blockData.subarray(id * meta.samples_per_line, (id+1) * meta.samples_per_line)
}

/** Get a single line from a given block number in the data section. */ 
export function getLineInBlock(lineId: number, blockId: number, data: Uint16Array, params) {
  return getLine(lineId, getBlock(blockId, data, params), params)
}

export function getDataSection(buf, meta) {
}
