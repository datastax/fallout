#!/bin/bash

# Bootstrap the wrapper with an appropriate JAVA_HOME

set -e

thisdir="$(cd "$(dirname "$0")"; pwd)"

export JAVA_HOME=${JAVA11_HOME:?This branch requires JAVA11_HOME to be set}
exec "$thisdir/gradle/gradlew" "$@"
