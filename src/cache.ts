/** Block cache. */

type Entry = {
  key: any;
  buf: ArrayBuffer;
}

export type Cache = {
  entries: Entry[];
  used: number;
  capacity: number;     // Max size in bytes.
  stats: {
    hits: number;       // Count of successful gets.
    misses: number;     // Count of unsuccessful gets.
    inserts: number;    // Count of insert operations.
    deletes: number;    // Count of delete operations.
    flushes: number;    // Count of free-up operations.
    retained: number;   // Number of bytes stored.
    released: number;   // Number of bytes freed.
  };
}

export function cache_create(capacity: number): Cache {
  return {
    entries: [],
    used: 0,
    capacity,
    stats: { hits: 0, misses: 0, inserts: 0, deletes: 0, flushes: 0, retained: 0, released: 0 },
  }
}

/** Add a block to the cache. Returns false if it can't fit. */
export function cache_add(key: any, buf: ArrayBuffer, cache: Cache): boolean {
  if(buf.byteLength > cache.capacity)
    return false
  if(buf.byteLength > cache.capacity - cache.used)
    flush(cache, buf.byteLength)
    
  const entry = {key, buf}
  cache.entries.unshift(entry)
  cache.used += buf.byteLength
  cache.stats.inserts++
  cache.stats.retained += buf.byteLength
  return true
}

/** Get a block from the cache, or null if not available. */
export function cache_get(key: any, cache: Cache): ArrayBuffer {
  const index = cache.entries.findIndex(entry => entry.key == key)
  if(index != -1) {
    const entry = cache.entries[index]
    cache.stats.hits++
    cache.entries.splice(index, 1)
    cache.entries.unshift(entry)
    return entry.buf
  } else {
    cache.stats.misses++
    return null
  }
}

/** Delete least-recently-used entries to free up space.
 * 
 * The optional `target` argument specifies the minimum number of bytes to be
 * available after the flush operation. Returns true if the target is met.
 */
function flush(cache: Cache, target=0) {
  cache.stats.flushes++
  while(cache.capacity - cache.used < target && cache.entries.length > 0) {
    const entry = cache.entries.pop()
    cache.stats.released += entry.buf.byteLength
    cache.stats.deletes++
    cache.used -= entry.buf.byteLength
  }
  return cache.capacity - cache.used >= target
}

/** Print debugging statistics to stderr. */
export function print_cache_stats(cache: Cache) {
  console.warn(`Cache stats: hits=${cache.stats.hits} misses=${cache.stats.misses} inserts=${cache.stats.inserts} flushes=${cache.stats.flushes} deletes=${cache.stats.deletes} retained=${cache.stats.retained} released=${cache.stats.released}`)
}