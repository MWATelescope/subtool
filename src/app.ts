import * as fs from 'node:fs/promises'
import { parse_command_line } from './cli.js'
import * as dt from './dtv2.js'
import * as dump from './dump.js'
import { load_subfile, overwrite_delay_table, overwrite_samples, resample_metadata, upgrade_delay_table, write_subfile } from './subfile.js'
import { print_header, parse_header, read_header, serialise_header, set_header_value } from './header.js'
import { init_metadata, write_section, ok, fail, is_ok, fail_with, async_arrow, fmap, await_all } from './util.js'
import type { Metadata, OutputDescriptor, Result, SourceMap, TransformerSet, TransformSpec } from './types'
import { FileHandle } from 'node:fs/promises'
import {make_phase_gradient_resampler, make_resampler_transform} from './resample.js'
import {cache_create, print_cache_stats} from './cache.js'
import {extract_source} from './dump.js'
import {bake_delays, upsample} from './dsp.js'
import {read_block, read_line, read_section} from './reader.js'


async function main(args: string[]) {
  const parseResult = parse_command_line(args)
  if(parseResult.status != 'ok')
    return parseResult
  
  switch(parseResult.command) {
  case 'show':
    return runShow(parseResult.fixedArgs[0], parseResult.opts)
  case 'dt':
    return runDt(parseResult.fixedArgs[0], parseResult.opts)
  case 'info':
    return runInfo(parseResult.fixedArgs[0], parseResult.opts)
  case 'dump':
    return dump.runDump(parseResult.fixedArgs[0], parseResult.fixedArgs[1], parseResult.opts)
  case 'repoint':
    return runRepoint(parseResult.fixedArgs[0], parseResult.fixedArgs[1], parseResult.opts)
  case 'replace':
    return runReplace(parseResult.fixedArgs[0], parseResult.fixedArgs[1], parseResult.opts)
  case 'get':
    return runGet(parseResult.fixedArgs[0], parseResult.fixedArgs[1], parseResult.opts)
  case 'set':
    return runSet(parseResult.fixedArgs[0], parseResult.fixedArgs[1], parseResult.fixedArgs[2], parseResult.opts)
  case 'unset':
    return runUnset(parseResult.fixedArgs[0], parseResult.fixedArgs[1], parseResult.opts)
  case 'resample':
    return runResample(parseResult.fixedArgs[0], parseResult.fixedArgs[1], parseResult.opts)
  case 'bake':
    return runBake(parseResult.fixedArgs[0], parseResult.opts)
  case 'patch':
    return runPatch(parseResult.fixedArgs[0], parseResult.fixedArgs[1], parseResult.opts)
  case 'upgrade':
    return runUpgrade(parseResult.fixedArgs[0], parseResult.opts)
  case 'upsample':
    return runUpsample(parseResult.fixedArgs[0], parseResult.fixedArgs[1], parseResult.opts)
  case null:
    return ok()
  default:
    console.error(`${parseResult.command} is not implemented.`)
    break
  }
  return ok()
}

async function runGet(key: string, filename: string, opts: any) {
  const loadResult = await load_subfile(filename)
  if(loadResult.status != 'ok')
    return loadResult
  const {meta, file, cache} = loadResult.value
  const headerResult = await read_header(file, meta, cache)
  if(headerResult.status != 'ok')
    return headerResult
  const header = headerResult.value
  if(key in header)
    console.log(header[key])
  else
    console.log(`No such key: ${key}.`)

  await file.close()
  return ok()
}

async function runSet(key: string, value: string, filename: string, opts: any) {
  const loadResult = await load_subfile(filename, 'r+')
  if(loadResult.status != 'ok')
    return loadResult
  const {meta, file, cache} = loadResult.value
  const headerResult = await read_header(file, meta, cache)
  if(headerResult.status != 'ok')
    return headerResult

  const header = headerResult.value
  const setResult = set_header_value(key, value, header, opts.set_force)
  if(setResult.status != 'ok')
    return setResult

  const headerBuf = serialise_header(header, meta)
  const writeResult = await write_section('header', headerBuf, file, meta)
  if(writeResult.status != 'ok')
    return writeResult

  await file.close()
  return ok()
}

async function runUnset(key: string, filename: string, opts: any) {
  const loadResult = await load_subfile(filename, 'r+')
  if(loadResult.status != 'ok')
    return loadResult
  const {meta, file, cache} = loadResult.value
  const headerResult = await read_header(file, meta, cache)
  if(headerResult.status != 'ok')
    return headerResult

  const header = headerResult.value
  if(!(key in header || opts.unset_force)) {
    await file.close()
    return fail(`No such key ${key}.`)
  } 

  delete header[key]

  const headerBuf = serialise_header(header, meta)
  const writeResult = await write_section('header', headerBuf, file, meta)
  if(writeResult.status != 'ok')
    return writeResult

  await file.close()
  return {status: 'ok'}
}

async function runShow(filename: string, opts: any) {
  const file: FileHandle = await fs.open(filename, 'r')
  const loadResult = await load_subfile(filename)
  if(loadResult.status != 'ok')
    return loadResult
  const {meta, cache} = loadResult.value

  // Read header
  // We always do this, even if we're not printing it, since everything else depends on it (except
  // when only printing the header in binary, but we're not optimising that special case).
  const headerBuf = new ArrayBuffer(4096)
  let result = await file.read(new Uint8Array(headerBuf), 0, 4096)
  if(result.bytesRead != 4096) throw `Failed to read header data. Expected to read 4096 bytes, got ${result.bytesRead}`
  const header = parse_header(headerBuf)
  if(opts.show_header) {
    console.log('Header section:')
    print_header(header, headerBuf, opts)
  }

  if(!(opts.show_delay_table || opts.show_data))
    return {status: 'ok'}

  // Read tile metadata
  console.log('')
  //const delayTableLength = 20 + 2 * 1600
  //const delayTablePad = 0 // 4 - delayTableLength % 4
  //const delayTableSectionLength = (delayTableLength + delayTablePad) * header.NINPUTS
  //const delayTableBuf = new ArrayBuffer(delayTableSectionLength)
  //result = await file.read(new Uint8Array(delayTableBuf), 0, delayTableSectionLength)
  //if(result.bytesRead != delayTableSectionLength)
  //  throw `Failed to read header data. Expected to read ${delayTableSectionLength} bytes, got ${result.bytesRead}`
  const tiles = meta.delay_table
  //for(let i=0; i<header.NINPUTS; i++) {
  //  const view = new DataView(delayTableBuf, i*(delayTableLength+delayTablePad), delayTableLength)
  //  const tile = dt.parse_delay_table_row(view, meta)
  //  tiles.push(tile)
  //}
  if(opts.show_delay_table) {
    console.log(dt.format_delay_table_pretty(meta.delay_table))
  }

  // Read voltages
  if(!opts.show_data) {
    await file.close()
    return {status: 'ok'}
  }
  const selected_ids = []
  if(opts.selected_sources != null) {
    for(let src of opts.selected_sources) {
      const idx = tiles.entries.findIndex(tile => tile.rf_input == src)
      if(idx == -1) {
        console.warn(`Warning: source ${src} not found in file.`)
        continue
      }
      selected_ids.push(idx)
    }
  } else {
    for(let i=0; i<tiles.entries.length; i++)
      selected_ids.push(i)
  }
  
  const dataBlockResult = await read_block(opts.show_block, file, meta, cache)
  if(dataBlockResult.status != 'ok')
    return dataBlockResult
  console.log('\nVoltage data:')
  const dataBlock = new Int8Array(dataBlockResult.value)
  const nSamples = Math.min(meta.samples_per_line, opts.num_samples)
  for(let i of selected_ids) {
    const xs = dataBlock.subarray(i*meta.sub_line_size, (i+1)*meta.sub_line_size)
    //let str = 
    process.stdout.write(`${tiles.entries[i].rf_input.toString().padStart(4, ' ')} `)
    for(let j=0; j<nSamples; j++) {
      let [re, im] = [xs[j*2], xs[j*2+1]]
      let [reStr, imStr] = [`${re}`, im >= 0 ? `+${im}` : `${im}`]
      process.stdout.write(`${reStr}${imStr}i `.padStart(8, ' '))
    }
    process.stdout.write('\n')
    //console.log(str)
  }

  await file.close()

  return {status: 'ok'}
}

async function runDt(filename: string, opts: any): Promise<Result<void>> {
  const result = await dt.load_delay_table(filename)
  if(!is_ok(result))
    return fail_with(result)
  const table = result.value
  if(opts.compare_file) {
    const cmpResult = await async_arrow(dt.load_delay_table, cmp => dt.compare_delay_tables(cmp, table))(opts.compare_file)
    if(!is_ok(cmpResult))
      return fail_with(cmpResult)
    const diff = cmpResult.value
    switch(opts.format_out) {
      case 'csv':
        console.log(dt.format_delay_table_csv(diff))
        break
      case 'pretty':
        console.log(dt.format_delay_table_pretty(diff, opts.dt_frac_digits, opts.dt_use_colour, opts.dt_version_out, opts.dt_allow_wrap))
        break
      case 'bin':
        fmap(buf => process.stdout.write(new Uint8Array(buf)), dt.serialise_delay_table(diff))
        break
      default:
        console.log(dt.format_delay_table_pretty(diff))
        break  
    }
  } else {
    switch(opts.format_out) {
      case 'csv':
        console.log(dt.format_delay_table_csv(table))
        break
      case 'pretty':
        console.log(dt.format_delay_table_pretty(table, opts.dt_frac_digits, opts.dt_use_colour, opts.dt_version_out, opts.dt_allow_wrap))
        break
      case 'bin':
        fmap(buf => process.stdout.write(new Uint8Array(buf)), dt.serialise_delay_table(table))
        break
      default:
        console.log(dt.format_delay_table_pretty(table))
        break  
    }
  }

  return ok()
}

async function runInfo(filename: string, opts: any) {
  const loadResult = await load_subfile(filename)
  if(loadResult.status != 'ok') {
    console.error(loadResult.reason)
    return
  }
  const meta = loadResult.value.meta
  Object.entries(meta).forEach(([k, v]) => {
    console.log(`${k}: ${v}`)
  })

  return {status: 'ok'}
}


async function runRepoint(infilename: string, outfilename: string, opts: any) {
  const loadResult = await load_subfile(infilename)
  if(loadResult.status != 'ok') {
    console.error(loadResult.reason)
    return
  }
  const {file, meta, cache} = loadResult.value

  const sectionsResult = await await_all([
    read_section('header', file, meta, cache),
    read_section('dt', file, meta, cache),
    read_section('udpmap', file, meta, cache),
    read_section('margin', file, meta, cache)
  ])
  if(!is_ok(sectionsResult))
    return fail_with(sectionsResult)
  const [headerBuf, dtBuf, udpmapBuf, marginBuf] = sectionsResult.value
  
  const dtMeta = init_metadata()
  dtMeta.filename = opts.delay_table_filename
  const loadDtResult = await dt.load_delay_table(opts.delay_table_filename)
  if(loadDtResult.status != 'ok') {
    console.error(loadDtResult.reason)
    return
  }

  const origDt = meta.delay_table
  const newDt = loadDtResult.value
  newDt.format_version = origDt.format_version
  const newDtBinResult = dt.serialise_delay_table(newDt)
  if(!is_ok(newDtBinResult))
    return newDtBinResult
  const newDtBin = newDtBinResult.value

  const outputMeta: Metadata = { ...meta, filename: outfilename}
  const outputDescriptor: OutputDescriptor = {
    meta: outputMeta,
    repoint: {
      from: origDt,
      to: newDt,
      margin: new Uint16Array(marginBuf),
    },
    sections: {
      header: { content: headerBuf, type: 'buffer' },
      dt: { content: newDtBin, type: 'buffer' },
      udpmap: { content: udpmapBuf, type: 'buffer' },
      margin: { content: marginBuf, type: 'buffer' },
      data: { file, type: 'file' },
    },
  }
  const writeResult = await write_subfile(outputDescriptor, opts, cache)
  if(writeResult.status != 'ok')
    return writeResult

  return {status: 'ok'}
}

async function runReplace(infilename: string, outfilename: string, opts: any) {
  const loadResult = await load_subfile(infilename)
  if(loadResult.status != 'ok') {
    console.error(loadResult.reason)
    return
  }
  const {file, meta, cache} = loadResult.value
  const sectionsResult = await await_all([
    read_section('header', file, meta, cache),
    read_section('dt', file, meta, cache),
    read_section('udpmap', file, meta, cache),
    read_section('margin', file, meta, cache)
  ])
  if(!is_ok(sectionsResult))
    return fail_with(sectionsResult)
  const [headerBuf, dtBuf, udpmapBuf, marginBuf] = sectionsResult.value

  const origMap: SourceMap = Object.fromEntries(meta.sources.map((x, i) => [x, i]))
  const remap: SourceMap = Object.fromEntries(meta.sources.map((x, i) => [x, i]))
  if(opts.replace_map_all != null) {
    for(let [k,v] of Object.entries(remap))
      remap[Number.parseInt(k)] = origMap[opts.replace_map_all]
  } else
    for(let [k,v] of opts.replace_map)
      remap[k] = origMap[v]
  
  
  const outputMeta = { ...meta, filename: outfilename}
  const outputDescriptor = {
    meta: outputMeta,
    remap,
    sections: {
      header: { content: headerBuf, type: 'buffer' },
      dt: { content: dtBuf, type: 'buffer' },
      udpmap: { content: udpmapBuf, type: 'buffer' },
      margin: { content: marginBuf, type: 'buffer' },
      data: { file, type: 'file' },
    },
  }
  const writeResult = await write_subfile(outputDescriptor, opts, cache)
  if(writeResult.status != 'ok')
    return writeResult

  return {status: 'ok'}
}

async function runResample(infilename: string, outfilename: string, opts: any) {
  const loadResult = await load_subfile(infilename)
  if(loadResult.status != 'ok') {
    console.error(loadResult.reason)
    return
  }
  const {file, meta, cache} = loadResult.value
  const sectionsResult = await await_all([
    read_section('header', file, meta, cache),
    read_section('dt', file, meta, cache),
    read_section('udpmap', file, meta, cache),
    read_section('margin', file, meta, cache)
  ])
  if(!is_ok(sectionsResult))
    return fail_with(sectionsResult)
  const [headerBuf, dtBuf, udpmapBuf, marginBuf] = sectionsResult.value

  const delayTable = meta.delay_table

  const rules: TransformerSet = {}
  for(let spec of opts.resample_rules) {
    const result = make_resampler_transform(spec.name, spec.args)
    if(result.status != 'ok')
      return fail_with(result)
    for(let source of spec.sources) {
      const idx = delayTable.entries.findIndex(row => row.rf_input == source)
      if(idx == -1)
        return fail(`RF Source ID ${source} not found in subfile.`)
      rules[idx] = result.value
    }
  }

  const outputMeta = { ...meta, filename: outfilename}
  const outputDescriptor = {
    meta: outputMeta,
    resample: {
      fns: rules,
      region: opts.resample_region,
    },
    sections: {
      header: { content: headerBuf, type: 'buffer' },
      dt: { content: dtBuf, type: 'buffer' },
      udpmap: { content: udpmapBuf, type: 'buffer' },
      margin: { content: marginBuf, type: 'buffer' },
      data: { file, type: 'file' },
    },
  }
  const writeResult = await write_subfile(outputDescriptor, opts, cache)
  if(writeResult.status != 'ok')
    return writeResult
  console.warn(`Wrote ${writeResult.bytesWritten} bytes to ${outfilename}.`)

  return {status: 'ok'}
}

async function runUpsample(ifname: string, ofname: string, opts: any): Promise<Result<void>> {
  const factor = opts.upsample_factor
  const cache = cache_create(2 ** 30) 
  const loadResult = await load_subfile(ifname, 'r', cache)
  if(loadResult.status != 'ok')
    return fail_with(loadResult)
  const oldContext = loadResult.value
  const createResult = await resample_metadata(factor, ofname, oldContext)
  if(createResult.status != 'ok')
    return fail_with(createResult)
  const newContext = createResult.value
  const outputLineLength = newContext.meta.sub_line_size * newContext.meta.blocks_per_sub * 2

  // Interate sources, resample and write source by source
  process.stderr.write('Upsampling sources...')
  for(let lineNum=0; lineNum<1; lineNum++) {
    const extractResult = await extract_source(lineNum, false, oldContext.file, oldContext.meta, cache)
    if(extractResult.status != 'ok')
      return fail_with(extractResult)
    
    const idata = new Int8Array(extractResult.value)
    const odata = new Int8Array(outputLineLength)

    upsample(idata, odata, newContext.meta, factor)
    await overwrite_samples(lineNum, odata, newContext.meta, newContext.file)
    
    process.stderr.write(` ${lineNum}`)
  }
  process.stderr.write(' ...done.\n')
  newContext.file.close()
  return ok()
}

async function runBake(ifname: string, opts: any): Promise<Result<void>> {
  const cache = cache_create(6 * 2 ** 30) // 6GB (whole subfile)
  const loadResult = await load_subfile(ifname, 'r+', cache)
  if(loadResult.status != 'ok')
    return fail_with(loadResult)
  const {file, meta} = loadResult.value
  process.stderr.write('Preloading sample data')
  for(let blockNum=1; blockNum<=meta.blocks_per_sub; blockNum++) {
    await read_block(blockNum, file, meta, cache)
    if(blockNum % 10 == 0)
      process.stderr.write('.')
  }
  process.stderr.write(' done.\n')
  process.stderr.write('Baking delays for sources...')
  for(let lineNum=0; lineNum<meta.num_sources; lineNum++) {
    const row = meta.delay_table.entries[lineNum]
    if(opts.bake_source != null && !opts.bake_source.includes(row.rf_input))
      continue
    const extractResult = await extract_source(lineNum, false, file, meta, cache)
    if(extractResult.status != 'ok')
      return fail_with(extractResult)
    const idata = new Int8Array(extractResult.value)
    const odata = new Int8Array(idata.byteLength)
    throw 'luke you need to fix this'
    //bake_delays(row.frac_delay, opts.bake_fft_size, idata, odata, meta)
    await overwrite_samples(lineNum, odata, meta, file)
    row.frac_delay.fill(0)
    process.stderr.write(` ${lineNum}`)
  }
  process.stderr.write(' ...done.\n')
  console.warn('Writing delay table.')
  await overwrite_delay_table(meta.delay_table, meta, file)
  print_cache_stats(cache)
  return ok()
}

async function runPatch(pfname: string, sfname: string, opts: any): Promise<Result<void>> {
  if(opts.patch_section == null)
    return fail(`Section to patch must be specified (--section=SECTION).`)
  if(opts.patch_section != 'dt')
    return fail(`Patching section '${opts.patch_section}' is not implemented.`)

  const subLoadResult = await load_subfile(sfname, 'r+')
  if(subLoadResult.status != 'ok')
    return fail_with(subLoadResult)
  const {file, meta} = subLoadResult.value

  const patchLoadResult = await dt.load_delay_table(pfname)
  if(patchLoadResult.status != 'ok')
    return fail_with(patchLoadResult)
  if(patchLoadResult.value.format_version != meta.mwax_sub_version)
    console.warn(`Warning: patching a V${meta.mwax_sub_version} subfile with a V${patchLoadResult.value.format_version} delay table. The delay table will be converted.`)
  patchLoadResult.value.format_version = meta.mwax_sub_version

  const writeResult = await overwrite_delay_table(patchLoadResult.value, meta, file)
  if(writeResult.status != 'ok')
    return fail_with(writeResult)

  console.warn(`Patched delay table in ${sfname} at 0x${meta.dt_offset.toString(16)} (${meta.dt_length} bytes).`)
  return ok()
}

async function runUpgrade(fname: string, opts: any): Promise<Result<void>> {
  const loadResult = await load_subfile(fname, 'r+')
  if(loadResult.status != 'ok')
    return fail_with(loadResult)
  const {file, meta, cache} = loadResult.value
  const result = await upgrade_delay_table(file, meta, cache)
  if(result.status != 'ok')
    return fail_with(result)
  console.warn(`Upgraded fractional delay precision for ${fname}.`)
  return ok()
}


const result: any = await main(process.argv.slice(2)) /*.catch(e => {
  console.error(`ERROR: ${e}`)
})*/

if(result.status != 'ok')
  console.error(`${result.reason}`)
