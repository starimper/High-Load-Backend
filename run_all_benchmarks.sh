#!/usr/bin/env bash
# Starts the server (separate process), runs bytes, latency, throughput, then reliability. Results go to ./results
cd "$(dirname "$0")"
./build.sh
./run_server.sh > results/server.log 2>&1 &
SERVER=$!
sleep 3
CP="target/pastebin-protocol.jar:lib/*"
for m in bytes latency throughput; do java -Xmx1g -cp "$CP" kz.kbtu.pastebin.bench.Benchmark $m --out results | tee results/$m.log; done
kill $SERVER
java -Dsun.net.httpserver.nodelay=true -Dlogical=2000 -cp "$CP" kz.kbtu.pastebin.bench.Benchmark reliability --out results | tee results/reliability.log
