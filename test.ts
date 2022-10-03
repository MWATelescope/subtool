/* to be restored and elaborated on. one day. maybe.

function test_timeshift() {
  const params = {
    MARGIN_SAMPLES: 8,
    BLOCKS_PER_SUB: 3,
    SAMPLES_PER_LINE: 4,
    NINPUTS: 2,
  }
  const blockData = Uint16Array.from([
    // Block 0 (not to be confused with the "block 0" metadata block in a real subfile)
      4,   5,   6,   7,    // Line 1
    104, 105, 106, 107,    // Line 2
    // Block 1
      8,   9,  10,  11,    // Line 1
    108, 109, 110, 111,    // Line 2
    // Block 2
     12,  13,  14,  15,    // Line 1
    112, 113, 114, 115,    // Line 2
  ])
  const marginData = Uint16Array.from([
      0,   1,   2,   3,     4,   5,   6,   7,    // Source 1 head margin
     12,  13,  14,  15,    16,  17,  18,  19,    // Source 1 tail margin
    100, 101, 102, 103,   104, 105, 106, 107,    // Source 2 head margin
    112, 113, 114, 115,   116, 117, 118, 119,    // Source 2 tail margin
  ])
  console.log('before: (shifts: -1/1)')
  printData(blockData, params)
  console.log('after: (shifts 2/-2)')
  const outBuf = new Uint16Array(blockData.length)
  timeShift(blockData, outBuf, [-1, 1], [2, -2], marginData, params)
  printData(outBuf, params)
}

function printData(data, params) {
  for(let line=0; line<params.NINPUTS; line++) {
    const buf = Array(params.BLOCKS_PER_SUB).fill(0).map((_,blk) =>
      Array.from(getLineInBlock(line, blk, data, params)).map(x => x.toString().padStart(3, ' ')).join(' ')
    )
    console.log(buf.join('   '))
  }
}


//test_timeshift()
*/