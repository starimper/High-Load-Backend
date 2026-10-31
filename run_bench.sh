#!/usr/bin/env bash
cd "$(dirname "$0")"
for m in latency throughput; do
  java -Xmx1g -cp "target/pastebin-protocol.jar:lib/*" kz.kbtu.pastebin.bench.Benchmark $m --out results > results/$m.log 2>&1
done
echo done > results/bench.done
