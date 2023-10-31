// import fs promises
const FFT = await require('./node_modules/fft.js/lib/fft.js')
const line_len_bytes = 625 // (number of samples per second / samples per packet) * number of seconds / bits per byte
const num_lines = 288




window.addEventListener('load', async () => {
    // fetch the data file '/pmap'
  const packetData = await fetch('mwax03.pmap').then(res => res.arrayBuffer())
  const array = new Uint8Array(packetData, 0)
  
  // fetch the line names file '/names.txt'
  const names = await fetch('names.txt').then(res => res.text()).then(text => text.split('\n').map(name => name.replace(/" /g, '')))
  //const names = (await fs.readFile('names.txt', 'utf8')).split('\n').map(name => name.replace(/" /g, ''))
  
  // split the array into lines of length line_len
  const lines = []
  for (let i = 0; i < num_lines; i += 1) {
    const pos = i * line_len_bytes
    lines.push(array.slice(pos, pos + line_len_bytes))
  }
  
  // replace the bytes in each line with a count of bits set in the byte
  lines.forEach(line => {
    for (let i = 0; i < line.length; i += 1) {
      let count = 0
      for (let j = 0; j < 8; j += 1) {
        count += (line[i] >> j) & 1
      }
      line[i] = count
    }
  })
  



  // sum all the bytes in each line
  const sums = lines.map(line => line.reduce((a, b) => a + b))
  
  // print the line numbers and line lengths
  console.log(sums.map((sum, i) => `${i} ${names[i]}:  ${sum}`).join('\n'))

  const fft = new FFT(1024)
  let istorage = fft.createComplexArray() //fft.createArray()
  let ostorage = fft.createComplexArray()

  const canvas = document.getElementById('canvas')
  canvas.height = num_lines
  canvas.width = 1024
  
  // get the canvas as a writeable bitmap
  const ctx = canvas.getContext('2d')
  const imageData = ctx.getImageData(0, 0, canvas.width, canvas.height)
  const data = imageData.data

  // write the lines to the canvas
  for (let i = 0; i < num_lines; i += 1) {
   // istorage.set(lines[i])
    const lineData = [...lines[i].map(x => 8-x), ...Array(399).fill(0)]
    ostorage.fill(0)
    istorage.fill(0)
    istorage = fft.toComplexArray(lineData, istorage)
    fft.transform(ostorage, istorage)
    const output = fft.fromComplexArray(ostorage)
    for (let j = 0; j < 1024; j += 1) {
      const pos = (i * 1024 + j) * 4
      //let val = 0 //lines[i][j]
      let re = output[j*2]
      let im = output[j*2+1]
      let mag = Math.min(255, Math.sqrt(re*re + im*im) * 2)
      let val = lineData[j] * 255/8
      //val *= 64 / 8
      data[pos] = val
      data[pos + 1] = val
      data[pos + 2] = val
      data[pos + 3] = 255
    }
  }

  // draw the canvas
  ctx.putImageData(imageData, 0, 0)


})


/** sum all the bytes in the array */

