/** dsp.ts -- Some basic DSP functionality for voltage data. 
 * 
 * Works on exported voltage stream files, which are just a list of complex
 * samples. Since they're only ever ~20mb or so, we load the whole waveform
 * into memory to work on it.
 */

import {open, readFile, writeFile} from "fs/promises";
import FFT from 'fft.js'
import {BlockTransform, DelayTable, Metadata, Result, Z} from "./types";
import {complex_rotate, fail, ok} from "./util.js";
import {load_delay_table} from "./dt.js";

export async function runDsp(rule: string, ifname: string, ofname: string): Promise<Result<void>> {
  const ibuf = await readFile(ifname)
  const idata = new Int8Array(ibuf)
  const odata = new Int8Array(idata.byteLength)

  //run_frac_delay_correction(62, 'dt.csv', idata, odata)

  await writeFile(ofname, Buffer.from(odata.buffer))
  return ok()
}

export function bake_delays(delays: Int32Array, fftsize: number, idata: Int8Array, odata: Int8Array, meta: Metadata): Result<void> {
  const sample_rate = meta.sample_rate
  const filter = make_frac_delay_filter(delays, 157000000, sample_rate*8, fftsize, sample_rate)
  apply_block_transform(filter, fftsize, idata, odata)
  return ok()
}

export function upsample(idata: Int8Array, odata: Int8Array, meta: Metadata, factor: number): Result<void> {
  const input_fft_size = 4096
  const output_fft_size = 8192
  const filter = make_upsampling_filter(input_fft_size, output_fft_size)
  apply_resizing_block_transform(filter, input_fft_size, output_fft_size, idata, odata)
  return ok()
}

function apply_block_transform(fn: BlockTransform, size: number, idata: Int8Array, odata: Int8Array): void {
  //if(idata.length % size != 0)
  //  console.warn(`Warning: applying block transform to data that is not a multiple in length of the block size.`)

  const nblocks = Math.floor(idata.length / (2*size))
  for(let i=0; i<nblocks; i++) {
    const iblock = idata.subarray(i*size*2, (i+1)*size*2)
    const oblock = odata.subarray(i*size*2, (i+1)*size*2)
    fn(iblock, oblock, i)
  }
  //throw "bang"
}

/** Apply a block transform with different input and output block sizes. */
function apply_resizing_block_transform(fn: BlockTransform, isize: number, osize: number, idata: Int8Array, odata: Int8Array): void {
  const nblocks = Math.floor(idata.length / (2*isize))
  for(let i=0; i<nblocks; i++) {
    const iblock = idata.subarray(i*isize*2, (i+1)*isize*2)
    const oblock = odata.subarray(i*osize*2, (i+1)*osize*2)
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
function make_frac_delay_filter(delays: Int32Array, centre: number, stream_len: number, fft_size: number, sample_rate: number): BlockTransform {
  const fft = new FFT(fft_size)
  const istorage = fft.createComplexArray()
  const ostorage = fft.createComplexArray()
  const fft_len = fft_size / sample_rate
  const chan_bw = 1 / fft_len

  function filter(idata: Int8Array, odata: Int8Array, blockIdx: number): void {
    // Where are we in the stream? Use the middle sample of the block and find the nearest delay value.
    const middleSample = blockIdx * fft_size + fft_size / 2
    const delayIdx = Math.floor(delays.length * middleSample / stream_len)
    const microsampleDelay = delays[delayIdx]
    const delay = microsampleDelay/1000000 / sample_rate
    const dcOffset = centre * delay * Math.PI * 2
    fft.transform(istorage, idata)
 
    for(let sampleIdx=0; sampleIdx < fft_size; sampleIdx++) {
      const freq = sampleIdx / (fft_size * fft_len)
      const fineOffset = freq * delay * Math.PI * 2
      const offset = dcOffset - fineOffset
      const oldSample: Z = [istorage[sampleIdx*2], istorage[sampleIdx*2+1]]
      const newSample = complex_rotate(-offset, oldSample)
      istorage[sampleIdx*2] = newSample[0]
      istorage[sampleIdx*2+1] = newSample[1]
    }
    fft.inverseTransform(ostorage, istorage)
    odata.set(ostorage)
  }
  return filter
}

function generate_complex_noise(idata: Int8Array, odata: Int8Array) {
  for(let i=0; i<odata.length; i++) {
    odata[i] = -128 + Math.floor(Math.random() * 256)
  }  
}

/** Apply upsampling.
 * 
 * This filter takes a stream of data and FFTs chunks of it, extends the range of
 * frequencies, and then inverse-FFT's the result. This is useful for increasing
 * the sample rate of a signal without introducing high-frequency noise.
 * 
 * fft_in:  Number of points in input FFT.
 * fft_out: Number of points in output inverse-FFT.
 */
function make_upsampling_filter(fft_size_in: number, fft_size_out: number) {
  const input_fft = new FFT(fft_size_in)
  const output_fft = new FFT(fft_size_out)
  const istorage = input_fft.createComplexArray()
  const ostorage = output_fft.createComplexArray()
  function filter(idata: Int8Array, odata: Int8Array, blockIdx: number): void {
    input_fft.transform(istorage, idata)
    //for(let sampleIdx=0; sampleIdx < fft_size_in; sampleIdx++) {
    //  const freq = sampleIdx / fft_size_in
    //  const newFreq = freq * fft_size_out / fft_size_in
    //  const newSampleIdx = Math.floor(newFreq * fft_size_in)
    //  ostorage[newSampleIdx*2] = istorage[sampleIdx*2]
    //  ostorage[newSampleIdx*2+1] = istorage[sampleIdx*2+1]
    //}
    output_fft.inverseTransform(ostorage, istorage)
    odata.set(ostorage)
  }
  return filter
}
