#!/bin/bash
set -euo pipefail
arch=$(dpkg --print-architecture)
/usr/bin/python3 /opt/xinghuo/build/fetch-source.py "java-$arch" /tmp/java.tar.gz
mkdir -p /opt/java
tar -xzf /tmp/java.tar.gz --strip-components=1 -C /opt/java
/usr/bin/python3 /opt/xinghuo/build/fetch-source.py ghidra /tmp/ghidra.zip
unzip -q /tmp/ghidra.zip -d /tmp/ghidra
mv /tmp/ghidra/ghidra_* /opt/ghidra
if [ "$arch" = arm64 ]; then
  # The official release includes Linux x86-64 native binaries only. Build the
  # upstream-supplied native sources for ARM64, rather than shipping a JVM-only
  # installation whose decompiler cannot start. The Gradle distribution is
  # checksum-pinned just like Ghidra and the JDK.
  /usr/bin/python3 /opt/xinghuo/build/fetch-source.py gradle /tmp/gradle.zip
  unzip -q /tmp/gradle.zip -d /tmp/gradle
  cd /opt/ghidra/support/gradle
  GRADLE_USER_HOME=/tmp/ghidra-gradle /tmp/gradle/gradle-*/bin/gradle \
    --no-daemon --max-workers="${BUILD_JOBS:-2}" \
    -Dorg.gradle.jvmargs=-Xmx512m buildNatives
  test -x /opt/ghidra/Ghidra/Features/Decompiler/os/linux_arm_64/decompile
  rm -rf /tmp/gradle /tmp/gradle.zip /tmp/ghidra-gradle
fi
rm -rf /tmp/ghidra /tmp/ghidra.zip /tmp/java.tar.gz
