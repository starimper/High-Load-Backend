#!/usr/bin/env bash
cd "$(dirname "$0")"
exec java -Dsun.net.httpserver.nodelay=true -Xmx2g -cp "target/pastebin-protocol.jar:lib/*" kz.kbtu.pastebin.ServerMain --threads 16
