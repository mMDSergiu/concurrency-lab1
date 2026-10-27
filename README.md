# Lab 1 – Concurrency in Practice (Node.js)

Author: Cătănoi Sergiu. Single program: `Lab1_Cătănoi_Sergiu.js` (Node.js ≥ 18, no dependencies, no build step).
Threads are `worker_threads` (real OS threads); locks, the queue and shared counters use `SharedArrayBuffer` + `Atomics`.

## Data
```
node Lab1_Cătănoi_Sergiu.js gen   --out numbers.txt                 # 50 000 000 random ints in [-1000000, 1000000]
node Lab1_Cătănoi_Sergiu.js split --file numbers.txt --dir D:\lab1_task2   # 8 files numbers_1..8.txt, 6 250 000 lines each
node Lab1_Cătănoi_Sergiu.js merge --dir D:\lab1_task2 --out numbers.txt    # inverse of split
```

## Tasks
```
node Lab1_Cătănoi_Sergiu.js info                                          # hardware to put in the report
node Lab1_Cătănoi_Sergiu.js 1a --file numbers.txt --threads 1|2|4|8|16|32|64
node Lab1_Cătănoi_Sergiu.js 1b --file numbers.txt --mode race|lock|local  # --threads defaults to logical processors; run race 3 times
node Lab1_Cătănoi_Sergiu.js 1c --file numbers.txt --workers 1|2|4|8|16|32|64
node Lab1_Cătănoi_Sergiu.js 2  --dir D:\lab1_task2 --threads 1|2|4        # files on the USB drive
node Lab1_Cătănoi_Sergiu.js bench --file numbers.txt                      # 1a, 1b, 1c in one go -> results.csv (time, memory, speed-up)
```
Task 2 must be run by hand: eject, unplug and replug the drive before **every** run.
Each run prints the time, peak memory (max RSS of the process) and the negative/positive/zero counts, and checks that they sum to 50 000 000.
