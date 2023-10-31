// import fs promises
import { promises as fs } from 'fs'

const line_len_bytes = 625 // (number of samples per second / samples per packet) * number of seconds / bits per byte
const num_lines = 288

// read the data file
const data = await fs.readFile('pmap')
const array = new Uint8Array(data.buffer, 0)

// read the line names file
const names = (await fs.readFile('names.txt', 'utf8')).split('\n').map(name => name.replace(/" /g, ''))

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




/** sum all the bytes in the array */

