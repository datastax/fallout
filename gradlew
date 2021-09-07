#!/bin/sh -e

# This redirects to the _actual_ wrapper with a workaround for
# https://github.com/diffplug/spotless/issues/834#issuecomment-815098368;
# once that is fixed, this is no longer necessary

thisdir="$(cd "$(dirname "$0")" && pwd)"

exec "$thisdir/gradle/gradlew" '-Dorg.gradle.jvmargs=--add-exports jdk.compiler/com.sun.tools.javac.api=ALL-UNNAMED --add-exports jdk.compiler/com.sun.tools.javac.file=ALL-UNNAMED --add-exports jdk.compiler/com.sun.tools.javac.parser=ALL-UNNAMED --add-exports jdk.compiler/com.sun.tools.javac.tree=ALL-UNNAMED --add-exports jdk.compiler/com.sun.tools.javac.util=ALL-UNNAMED' "$@"
