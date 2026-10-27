'use strict';
// Laboratory Work 1: Concurrency in Practice  (Node.js, worker_threads)
// Author: Cătănoi Sergiu
//
// One program, every task selected by command-line parameters:
//
//   node Lab1_Cătănoi_Sergiu.js info
//   node Lab1_Cătănoi_Sergiu.js gen   --out numbers.txt                  (50 000 000 random ints)
//   node Lab1_Cătănoi_Sergiu.js split --file numbers.txt --dir <dir>     (8 files, 6 250 000 lines each)
//   node Lab1_Cătănoi_Sergiu.js merge --dir <dir> --out numbers.txt      (inverse of split)
//   node Lab1_Cătănoi_Sergiu.js 1a --file numbers.txt --threads N
//   node Lab1_Cătănoi_Sergiu.js 1b --file numbers.txt --mode race|lock|local [--threads N] [--limit M]
//   node Lab1_Cătănoi_Sergiu.js 1c --file numbers.txt --workers N
//   node Lab1_Cătănoi_Sergiu.js 2  --dir <dir with numbers_1..8.txt> --threads N
//   node Lab1_Cătănoi_Sergiu.js bench --file numbers.txt [--out results.csv]   (1a, 1b, 1c in one go)
//
// Every counting task prints: time, peak memory (max RSS of the whole process, all threads
// share it), the three counts and whether their sum is 50 000 000.

const { Worker, isMainThread, parentPort, workerData } = require('worker_threads');
const fs = require('fs');
const path = require('path');
const os = require('os');

const TOTAL = 50000000;
const PER_FILE = 6250000;
const FILES = 8;
const MIN = -1000000;
const MAX = 1000000;

const CHUNK = 1 << 20;        // read buffer of the counting threads
const BATCH_LINES = 10000;    // Task 1c: lines per batch
const QUEUE_CAP = 100;        // Task 1c: max batches in the queue
const SLOT_BYTES = BATCH_LINES * 12; // a line is at most 10 bytes ("-1000000\r\n")

// ---------------------------------------------------------------------------
// Parsing / counting
// ---------------------------------------------------------------------------

// st = Int32Array [neg, pos, zero, sign, value, hasDigits]; keeps the number that is
// being parsed so that it survives a chunk boundary.
function newState() { return new Int32Array(6); }

function scan(buf, from, to, st) {
  let neg = st[0], pos = st[1], zero = st[2], sign = st[3], val = st[4], has = st[5];
  for (let i = from; i < to; i++) {
    const b = buf[i];
    if (b >= 48 && b <= 57) { val = val * 10 + (b - 48); has = 1; }
    else if (b === 45) sign = 1;
    else if (b === 10) {
      if (has) { if (val === 0) zero++; else if (sign) neg++; else pos++; }
      sign = 0; val = 0; has = 0;
    }
  }
  st[0] = neg; st[1] = pos; st[2] = zero; st[3] = sign; st[4] = val; st[5] = has;
}

// Last line of a file without a trailing newline.
function flush(st) {
  if (st[5]) { if (st[4] === 0) st[2]++; else if (st[3]) st[0]++; else st[1]++; }
  st[3] = 0; st[4] = 0; st[5] = 0;
}

// Task 1a: count the lines that START in [s, e). A line is owned by the thread whose range
// contains its first byte, so no line is counted twice or skipped:
//  - the thread starts at byte s-1; everything up to and including the first '\n' belongs
//    to a line that started earlier (or, if byte s-1 is '\n', is just that newline);
//  - it stops after the first '\n' found at offset >= e-1, i.e. it finishes the line that
//    covers byte e-1, which the next thread skips in the same way.
function countRange(fd, s, e, st) {
  const chunk = Buffer.allocUnsafe(CHUNK);
  let pos = s > 0 ? s - 1 : 0;
  let skipping = s > 0;
  for (;;) {
    const n = fs.readSync(fd, chunk, 0, CHUNK, pos);
    if (n === 0) break;
    const view = chunk.subarray(0, n);
    let from = 0, to = n, done = false;
    if (skipping) {
      const nl = view.indexOf(10);
      if (nl === -1) { pos += n; continue; }
      skipping = false;
      from = nl + 1;
      if (pos + from >= e) break;   // the next line starts in the next thread's part
    }
    const idx = view.indexOf(10, Math.max(from, e - 1 - pos));
    if (idx !== -1) { to = idx + 1; done = true; }
    scan(chunk, from, to, st);
    if (done) break;
    pos += n;
  }
  flush(st);
}

// Parse the whole file into an Int32Array (Task 1b, not measured).
function loadNumbers(file, limit) {
  const size = fs.statSync(file).size;
  const fd = fs.openSync(file, 'r');
  const cap = Math.min(limit, TOTAL + 1000000);
  const sab = new SharedArrayBuffer(cap * 4);
  const out = new Int32Array(sab);
  const chunk = Buffer.allocUnsafe(CHUNK);
  let n = 0, sign = 0, val = 0, has = 0, pos = 0;
  outer: while (pos < size) {
    const r = fs.readSync(fd, chunk, 0, CHUNK, pos);
    if (r === 0) break;
    pos += r;
    for (let i = 0; i < r; i++) {
      const b = chunk[i];
      if (b >= 48 && b <= 57) { val = val * 10 + (b - 48); has = 1; }
      else if (b === 45) sign = 1;
      else if (b === 10) {
        if (has) { out[n++] = sign ? -val : val; if (n === cap) break outer; }
        sign = 0; val = 0; has = 0;
      }
    }
  }
  if (has && n < cap) out[n++] = sign ? -val : val;
  fs.closeSync(fd);
  return { sab, n };
}

// ---------------------------------------------------------------------------
// Synchronisation primitives on SharedArrayBuffer (Atomics)
// ---------------------------------------------------------------------------

// Three-state futex mutex: 0 free, 1 locked, 2 locked with waiters.
function mutexLock(a, i) {
  let c = Atomics.compareExchange(a, i, 0, 1);
  if (c !== 0) {
    if (c !== 2) c = Atomics.exchange(a, i, 2);
    while (c !== 0) {
      Atomics.wait(a, i, 2);
      c = Atomics.exchange(a, i, 2);
    }
  }
}
function mutexUnlock(a, i) {
  if (Atomics.sub(a, i, 1) !== 1) { Atomics.store(a, i, 0); Atomics.notify(a, i, 1); }
}

// Bounded blocking queue of byte batches: a ring of QUEUE_CAP slots, one mutex and two
// condition variables (not-full / not-empty, each a sequence counter for Atomics.wait).
// ctl: [0] mutex [1] head [2] tail [3] count [4] closed [5] notFull seq [6] notEmpty seq
const Q_LOCK = 0, Q_HEAD = 1, Q_TAIL = 2, Q_COUNT = 3, Q_CLOSED = 4, Q_NOTFULL = 5, Q_NOTEMPTY = 6;

class SharedQueue {
  constructor(ctlSab, dataSab, lenSab) {
    this.ctl = new Int32Array(ctlSab);
    this.data = new Uint8Array(dataSab);
    this.lens = new Int32Array(lenSab);
  }
  static create() {
    return {
      ctlSab: new SharedArrayBuffer(8 * 4),
      dataSab: new SharedArrayBuffer(QUEUE_CAP * SLOT_BYTES),
      lenSab: new SharedArrayBuffer(QUEUE_CAP * 4),
    };
  }
  _wait(idx) {                      // called with the lock held; returns with it held
    const seq = Atomics.load(this.ctl, idx);
    mutexUnlock(this.ctl, Q_LOCK);
    Atomics.wait(this.ctl, idx, seq);
    mutexLock(this.ctl, Q_LOCK);
  }
  _signal(idx, n) { Atomics.add(this.ctl, idx, 1); Atomics.notify(this.ctl, idx, n); }

  push(src, len) {                  // blocks while the queue is full
    const c = this.ctl;
    mutexLock(c, Q_LOCK);
    while (c[Q_COUNT] === QUEUE_CAP) this._wait(Q_NOTFULL);
    const slot = c[Q_TAIL];
    this.data.set(src.subarray(0, len), slot * SLOT_BYTES);
    this.lens[slot] = len;
    c[Q_TAIL] = (slot + 1) % QUEUE_CAP;
    c[Q_COUNT]++;
    this._signal(Q_NOTEMPTY, 1);
    mutexUnlock(c, Q_LOCK);
  }
  pop(dst) {                        // blocks while empty; -1 when closed and drained
    const c = this.ctl;
    mutexLock(c, Q_LOCK);
    while (c[Q_COUNT] === 0) {
      if (c[Q_CLOSED]) { mutexUnlock(c, Q_LOCK); return -1; }
      this._wait(Q_NOTEMPTY);
    }
    const slot = c[Q_HEAD];
    const len = this.lens[slot];
    // Copy inside the lock: once the slot is released the reader may overwrite it.
    dst.set(this.data.subarray(slot * SLOT_BYTES, slot * SLOT_BYTES + len), 0);
    c[Q_HEAD] = (slot + 1) % QUEUE_CAP;
    c[Q_COUNT]--;
    this._signal(Q_NOTFULL, 1);
    mutexUnlock(c, Q_LOCK);
    return len;
  }
  close() {
    mutexLock(this.ctl, Q_LOCK);
    this.ctl[Q_CLOSED] = 1;
    this._signal(Q_NOTEMPTY, Infinity);
    mutexUnlock(this.ctl, Q_LOCK);
  }
}

// ---------------------------------------------------------------------------
// Worker side
// ---------------------------------------------------------------------------

function workerMain() {
  const w = workerData;
  switch (w.role) {
    case 'range': {                                   // Task 1a
      const fd = fs.openSync(w.file, 'r');
      const st = newState();
      countRange(fd, w.start, w.end, st);
      fs.closeSync(fd);
      parentPort.postMessage({ counts: [st[0], st[1], st[2]] });
      break;
    }
    case 'sum': {                                     // Task 1b
      const nums = new Int32Array(w.numsSab);
      const shared = new Int32Array(w.ctlSab);        // [0] mutex [1] start flag [4..6] counters
      const local = [0, 0, 0];
      parentPort.postMessage({ ready: true });
      Atomics.wait(shared, 1, 0);                     // wait for the start signal
      const from = w.from, to = w.to;
      if (w.mode === 'race') {
        for (let i = from; i < to; i++) {
          const v = nums[i];
          shared[v < 0 ? 4 : v > 0 ? 5 : 6]++;        // read-modify-write, NOT atomic
        }
      } else if (w.mode === 'lock') {
        for (let i = from; i < to; i++) {
          const v = nums[i];
          mutexLock(shared, 0);
          shared[v < 0 ? 4 : v > 0 ? 5 : 6]++;
          mutexUnlock(shared, 0);
        }
      } else {
        let a = 0, b = 0, c = 0;
        for (let i = from; i < to; i++) {
          const v = nums[i];
          if (v < 0) a++; else if (v > 0) b++; else c++;
        }
        local[0] = a; local[1] = b; local[2] = c;
      }
      parentPort.postMessage({ counts: local });
      break;
    }
    case 'reader': {                                  // Task 1c: producer
      const q = new SharedQueue(w.ctlSab, w.dataSab, w.lenSab);
      const fd = fs.openSync(w.file, 'r');
      const chunk = Buffer.allocUnsafe(CHUNK);
      const batch = Buffer.allocUnsafe(SLOT_BYTES);
      let have = 0, lines = 0, batchLen = 0;
      for (;;) {
        const n = fs.readSync(fd, chunk, have, CHUNK - have, null);
        if (n === 0) break;
        const total = have + n;
        let segStart = 0, lastNL = -1;
        for (let i = 0; i < total; i++) {
          if (chunk[i] === 10) {
            lastNL = i;
            if (++lines === BATCH_LINES) {
              chunk.copy(batch, batchLen, segStart, i + 1);
              batchLen += i + 1 - segStart;
              q.push(batch, batchLen);
              batchLen = 0; lines = 0; segStart = i + 1;
            }
          }
        }
        if (lastNL >= segStart) {
          chunk.copy(batch, batchLen, segStart, lastNL + 1);
          batchLen += lastNL + 1 - segStart;
          segStart = lastNL + 1;
        }
        have = total - segStart;                      // partial last line of the chunk
        chunk.copyWithin(0, segStart, total);
      }
      fs.closeSync(fd);
      if (have > 0) {                                 // final line without '\n'
        chunk[have] = 10; have++;
        chunk.copy(batch, batchLen, 0, have);
        batchLen += have;
      }
      if (batchLen > 0) q.push(batch, batchLen);
      q.close();
      parentPort.postMessage({ done: true });
      break;
    }
    case 'consumer': {                                // Task 1c: worker
      const q = new SharedQueue(w.ctlSab, w.dataSab, w.lenSab);
      const buf = Buffer.allocUnsafe(SLOT_BYTES);
      const st = newState();
      for (;;) {
        const len = q.pop(buf);
        if (len < 0) break;
        scan(buf, 0, len, st);                        // batches end on a line boundary
      }
      parentPort.postMessage({ counts: [st[0], st[1], st[2]] });
      break;
    }
    case 'files': {                                   // Task 2
      const counts = [0, 0, 0];
      for (const f of w.files) {
        const fd = fs.openSync(f, 'r');               // whole file: one read into one buffer
        const size = fs.fstatSync(fd).size;
        const buf = Buffer.allocUnsafe(size);
        let got = fs.readSync(fd, buf, 0, size, 0);
        while (got < size) got += fs.readSync(fd, buf, got, size - got, got); // only if the OS returns a short read
        fs.closeSync(fd);
        const st = newState();
        scan(buf, 0, buf.length, st);
        flush(st);
        counts[0] += st[0]; counts[1] += st[1]; counts[2] += st[2];
      }
      parentPort.postMessage({ counts });
      break;
    }
  }
}

// ---------------------------------------------------------------------------
// Main side
// ---------------------------------------------------------------------------

function parseArgs(argv) {
  const o = { _: [] };
  for (let i = 0; i < argv.length; i++) {
    if (argv[i].startsWith('--')) o[argv[i].slice(2)] = argv[++i];
    else o._.push(argv[i]);
  }
  return o;
}

// Start a worker; result resolves with its first {counts} / {done} message.
function spawn(data) {
  const w = new Worker(__filename, { workerData: data });
  const result = new Promise((resolve, reject) => {
    w.on('message', (m) => { if (m.counts || m.done) resolve(m); });
    w.on('error', reject);
    w.on('exit', (code) => { if (code !== 0) reject(new Error('worker exit ' + code)); });
  });
  return { w, result };
}

function sumCounts(results) {
  const t = [0, 0, 0];
  for (const r of results) if (r.counts) { t[0] += r.counts[0]; t[1] += r.counts[1]; t[2] += r.counts[2]; }
  return t;
}

function report(task, label, seconds, t, expected) {
  const sum = t[0] + t[1] + t[2];
  const mem = process.resourceUsage().maxRSS / 1024;
  const ok = sum === expected ? 'OK' : 'WRONG (expected ' + expected + ')';
  console.log(`task=${task} ${label} time=${seconds.toFixed(3)}s peakMem=${mem.toFixed(1)}MB ` +
    `negative=${t[0]} positive=${t[1]} zero=${t[2]} sum=${sum} ${ok}`);
}

const nowSec = () => Number(process.hrtime.bigint()) / 1e9;

async function task1a(o) {
  const file = o.file, threads = +o.threads || 1;
  const size = fs.statSync(file).size;
  const t0 = nowSec();
  const ws = [];
  for (let i = 0; i < threads; i++) {
    ws.push(spawn({ role: 'range', file, start: Math.floor(size * i / threads), end: Math.floor(size * (i + 1) / threads) }));
  }
  const res = await Promise.all(ws.map((x) => x.result));
  report('1a', `threads=${threads}`, nowSec() - t0, sumCounts(res), TOTAL);
}

async function task1b(o) {
  const threads = +o.threads || os.cpus().length;
  const mode = o.mode || 'local';
  if (!['race', 'lock', 'local'].includes(mode)) throw new Error('--mode must be race, lock or local');
  const limit = o.limit ? +o.limit : Infinity;
  const { sab, n } = loadNumbers(o.file, limit);   // not measured
  const ctlSab = new SharedArrayBuffer(16 * 4);
  const ctl = new Int32Array(ctlSab);
  const ws = [];
  const ready = [];
  for (let i = 0; i < threads; i++) {
    const w = new Worker(__filename, {
      workerData: { role: 'sum', mode, numsSab: sab, ctlSab, from: Math.floor(n * i / threads), to: Math.floor(n * (i + 1) / threads) },
    });
    ready.push(new Promise((r) => w.once('message', r)));
    ws.push({ w, result: new Promise((resolve, reject) => {
      w.on('message', (m) => { if (m.counts) resolve(m); });
      w.on('error', reject);
    }) });
  }
  await Promise.all(ready);
  const t0 = nowSec();
  Atomics.store(ctl, 1, 1);
  Atomics.notify(ctl, 1, Infinity);
  const res = await Promise.all(ws.map((x) => x.result));
  const seconds = nowSec() - t0;
  const t = mode === 'local' ? sumCounts(res) : [ctl[4], ctl[5], ctl[6]];
  report('1b', `mode=${mode} threads=${threads}`, seconds, t, n);
  for (const x of ws) x.w.terminate();
}

async function task1c(o) {
  const workers = +o.workers || 1;
  const qs = SharedQueue.create();
  const t0 = nowSec();
  const reader = spawn({ role: 'reader', file: o.file, ...qs });
  const ws = [];
  for (let i = 0; i < workers; i++) ws.push(spawn({ role: 'consumer', ...qs }));
  const res = await Promise.all([reader.result, ...ws.map((x) => x.result)]);
  report('1c', `workers=${workers}`, nowSec() - t0, sumCounts(res), TOTAL);
}

async function task2(o) {
  const threads = +o.threads || 1;
  const files = [];
  for (let i = 1; i <= FILES; i++) files.push(path.join(o.dir, `numbers_${i}.txt`));
  const t0 = nowSec();
  const ws = [];
  for (let i = 0; i < threads; i++) {              // as even as possible: sizes differ by at most 1
    const a = Math.floor(FILES * i / threads), b = Math.floor(FILES * (i + 1) / threads);
    ws.push(spawn({ role: 'files', files: files.slice(a, b) }));
  }
  const res = await Promise.all(ws.map((x) => x.result));
  report('2', `threads=${threads}`, nowSec() - t0, sumCounts(res), TOTAL);
}

// --- data preparation ------------------------------------------------------

function genFile(out) {
  const fd = fs.openSync(out, 'w');
  const buf = Buffer.allocUnsafe(8 << 20);
  const tmp = Buffer.allocUnsafe(12);
  let p = 0;
  for (let i = 0; i < TOTAL; i++) {
    if (p > buf.length - 16) { fs.writeSync(fd, buf, 0, p); p = 0; }
    let v = MIN + Math.floor(Math.random() * (MAX - MIN + 1));
    if (v < 0) { buf[p++] = 45; v = -v; }
    if (v === 0) buf[p++] = 48;
    else {
      let k = 0;
      while (v > 0) { tmp[k++] = 48 + (v % 10); v = (v / 10) | 0; }
      while (k > 0) buf[p++] = tmp[--k];
    }
    buf[p++] = 10;
  }
  fs.writeSync(fd, buf, 0, p);
  fs.closeSync(fd);
  console.log(`wrote ${TOTAL} numbers to ${out}`);
}

function splitFile(file, dir) {
  fs.mkdirSync(dir, { recursive: true });
  const data = fs.readFileSync(file);
  let start = 0, lines = 0, part = 1;
  for (let i = 0; i < data.length && part <= FILES; i++) {
    if (data[i] === 10 && ++lines === PER_FILE) {
      fs.writeFileSync(path.join(dir, `numbers_${part++}.txt`), data.subarray(start, i + 1));
      start = i + 1; lines = 0;
    }
  }
  console.log(`wrote ${part - 1} files to ${dir}`);
}

function mergeFiles(dir, out) {
  const fd = fs.openSync(out, 'w');
  for (let i = 1; i <= FILES; i++) fs.writeSync(fd, fs.readFileSync(path.join(dir, `numbers_${i}.txt`)));
  fs.closeSync(fd);
  console.log(`merged ${FILES} files into ${out}`);
}

function info() {
  const c = os.cpus();
  console.log(`CPU: ${c[0].model}`);
  console.log(`logical processors: ${c.length}`);
  console.log(`RAM: ${(os.totalmem() / 2 ** 30).toFixed(1)} GB`);
  console.log(`OS: ${os.type()} ${os.release()} ${os.arch()}`);
  console.log(`Node.js: ${process.version} (V8 ${process.versions.v8})`);
  if (process.platform === 'win32') {
    const ps = (cmd) => {
      try { return require('child_process').execFileSync('powershell', ['-NoProfile', '-Command', cmd], { encoding: 'utf8' }).trim(); }
      catch (e) { return '?'; }
    };
    console.log('physical cores: ' + ps('(Get-CimInstance Win32_Processor | Measure-Object NumberOfCores -Sum).Sum'));
    console.log('disks:\n' + ps('Get-PhysicalDisk | Format-Table FriendlyName,MediaType,BusType,@{n="GB";e={[int]($_.Size/1GB)}} -AutoSize | Out-String'));
  }
}

// Runs the measured configurations, each in its own process (peak memory is per process),
// and writes results.csv. Task 2 is not included: the USB drive must be replugged before
// every run, so run it by hand.
function bench(o) {
  const { execFileSync } = require('child_process');
  const file = o.file, rows = [];
  const runs = [];
  for (const n of [1, 2, 4, 8, 16, 32, 64]) runs.push(['1a', '--threads', n]);
  for (const m of ['race', 'race', 'race', 'lock', 'local']) runs.push(['1b', '--mode', m]);
  for (const n of [1, 2, 4, 8, 16, 32, 64]) runs.push(['1c', '--workers', n]);
  for (const r of runs) {
    const out = execFileSync(process.execPath, [__filename, r[0], '--file', file, r[1], String(r[2])], { encoding: 'utf8' }).trim();
    console.log(out);
    const m = /task=(\S+) (.*?) time=([\d.]+)s peakMem=([\d.]+)MB negative=(\d+) positive=(\d+) zero=(\d+) sum=(\d+) (\S+)/.exec(out);
    rows.push([m[1], m[2], m[3], m[4], m[5], m[6], m[7], m[8], m[9]]);
  }
  const base = {};
  const lines = ['task,config,time_s,peak_mem_mb,negative,positive,zero,sum,check,speedup_vs_first'];
  for (const r of rows) {
    if (!(r[0] in base)) base[r[0]] = +r[2];
    lines.push([...r, r[0] === '1b' ? '' : (base[r[0]] / +r[2]).toFixed(2)].join(','));
  }
  fs.writeFileSync(o.out || 'results.csv', lines.join('\n') + '\n');
  console.log('wrote ' + (o.out || 'results.csv'));
}

async function main() {
  const o = parseArgs(process.argv.slice(2));
  const cmd = o._[0];
  switch (cmd) {
    case 'info': return info();
    case 'gen': return genFile(o.out || 'numbers.txt');
    case 'split': return splitFile(o.file, o.dir);
    case 'merge': return mergeFiles(o.dir, o.out || 'numbers.txt');
    case '1a': return task1a(o);
    case '1b': return task1b(o);
    case '1c': return task1c(o);
    case '2': return task2(o);
    case 'bench': return bench(o);
    default:
      console.error('usage: node Lab1_Cătănoi_Sergiu.js info | gen | split | merge | 1a | 1b | 1c | 2  (see header)');
      process.exit(1);
  }
}

if (isMainThread) main().then(() => process.exit(0), (e) => { console.error(e); process.exit(1); });
else workerMain();
