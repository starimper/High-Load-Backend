#!/usr/bin/env bash
cd "$(dirname "$0")"
./build.sh
mkdir -p target/test-classes
javac --release 21 -cp "target/pastebin-protocol.jar:lib/*" -d target/test-classes src/test/java/kz/kbtu/pastebin/TestMain.java
java -cp "target/test-classes:target/pastebin-protocol.jar:lib/*" kz.kbtu.pastebin.TestMain
