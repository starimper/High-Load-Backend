#!/usr/bin/env bash
# Offline build without Maven: javac + jar. Output: target/pastebin-protocol.jar (run with lib/* on the classpath).
set -euo pipefail
cd "$(dirname "$0")"
rm -rf target/classes && mkdir -p target/classes
javac --release 21 -cp "lib/*" -d target/classes $(find src/main/java -name '*.java')
if [ -d src/main/resources ]; then cp -R src/main/resources/. target/classes/; fi
jar cf target/pastebin-protocol.jar -C target/classes .
echo "built target/pastebin-protocol.jar"
