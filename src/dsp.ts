/** dsp.ts -- Some basic DSP functionality for voltage data. 
 * 
 * Works on exported voltage stream files, which are just a list of complex
 * samples. Since they're only ever ~20mb or so, we load the whole waveform
 * into memory to work on it.
 */

import {open, readFile, writeFile} from "fs/promises";
import FFT from 'fft.js'
import {BlockTransform, DelayTable, Result, Z} from "./types";
import {complex_rotate, fail, ok} from "./util";
import {load_delay_table} from "./dt";

export async function runDsp(rule: string, ifname: string, ofname: string): Promise<Result<void>> {
  const ibuf = await readFile(ifname)
  const idata = new Int8Array(ibuf)
  const odata = new Int8Array(idata.byteLength)

  run_frac_delay_correction(62, 'dt.csv', idata, odata)

  await writeFile(ofname, Buffer.from(odata.buffer))
  return ok()
}

export async function run_frac_delay_correction(sourceId: number, dtfilename: string, idata: Int8Array, odata: Int8Array): Promise<Result<void>> {
  const dtResult: any = await load_delay_table(dtfilename, null, null)
  if(dtResult.status != 'ok')
    return fail(dtResult.reason)
  const delayTable: DelayTable = dtResult.table
  const rowIdx = delayTable.findIndex(row => row.rf_input == sourceId)
  if(rowIdx == -1)
    return fail(`Source ID ${sourceId} not found in delay table ${dtfilename}.`)
  const delays = delayTable[rowIdx].frac_delay
  const sample_rate = 1280000
  const filter = make_frac_delay_filter(delays, 157000000, sample_rate*8, 4096, sample_rate)
  apply_block_transform(filter, 4096, idata, odata)
  return ok()
}

function apply_block_transform(fn: BlockTransform, size: number, idata: Int8Array, odata: Int8Array): void {
  if(size % idata.length != 0)
    console.warn(`Warning: applying block transform to data that is not a multiple in length of the block size.`)

  const nblocks = Math.ceil(size / idata.length)
  for(let i=0; i<nblocks; i++) {
    const iblock = idata.subarray(i*size, (i+1)*size)
    const oblock = odata.subarray(i*size, (i+1)*size)
    fn(iblock, oblock, i)
  }
}

function make_band_pass_filter(fmin: number, fmax: number, bandwidth: number, fftsize: number): BlockTransform {
  const fft = new FFT(fftsize)
  const istorage = fft.createComplexArray()
  const ostorage = fft.createComplexArray()
  const lo = fftsize * fmin / bandwidth
  const hi = fftsize * fmax / bandwidth
  function filter(idata: Int8Array, odata: Int8Array, blockIdx: number): void {
    fft.transform(istorage, idata)
    for(let sampleIdx=0; sampleIdx < fftsize; sampleIdx++) {
      if(sampleIdx >= lo && sampleIdx < hi)
        continue
      istorage[sampleIdx*2] = 0
      istorage[sampleIdx*2+1] = 0
    }
    fft.inverseTransform(ostorage, istorage)
  }
  return filter
}

/** Apply millisample delays.
 * 
 * delays:      A list of time delays in millisamples.
 * centre:      Centre frequency in hertz.
 * stream_len:  Number of samples in stream (throughout which the delay list will be stepped).
 * fft_size:    Number of points in FFT.
 * sample_rate: Samples per second.
 */
function make_frac_delay_filter(delays: Int16Array, centre: number, stream_len: number, fft_size: number, sample_rate: number): BlockTransform {
  const fft = new FFT(fft_size)
  const istorage = fft.createComplexArray()
  const ostorage = fft.createComplexArray()
  const fft_len = fft_size / sample_rate

  function filter(idata: Int8Array, odata: Int8Array, blockIdx: number): void {
    // Where are we in the stream? Use the middle sample of the block and find the nearest delay value.
    const middleSample = blockIdx * (idata.length / 2) + idata.length / 4
    const delayIdx = Math.floor(delays.length * middleSample / stream_len)
    const millisampleDelay = delays[delayIdx]
    const delay = millisampleDelay/1000 / sample_rate
    const dcOffset = centre * delay * Math.PI * 2
    
    fft.transform(istorage, idata)
    for(let sampleIdx=0; sampleIdx < fft_size; sampleIdx++) {
      const freq = sampleIdx / (fft_size * fft_len)
      const fineOffset = freq * delay * Math.PI * 2
      const offset = fineOffset + dcOffset
      const oldSample: Z = [istorage[sampleIdx*2], istorage[sampleIdx*2+1]]
      const newSample = complex_rotate(offset, oldSample)
      istorage[sampleIdx*2] = newSample[0]
      istorage[sampleIdx*2+1] = newSample[1]
    }
    fft.inverseTransform(ostorage, istorage)
  }
  return filter
}

function generate_complex_noise(idata: Int8Array, odata: Int8Array) {
  for(let i=0; i<odata.length; i++) {
    odata[i] = -128 + Math.floor(Math.random() * 256)
  }  
}

