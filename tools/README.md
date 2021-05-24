# Extending fallout with standalone tools

This directory contains tools that run as standalone executables, invoked within fallout by the code in [`com.datastax.fallout.components.tools`](../src/main/java/com/datastax/fallout/components/tools).

The source code is organised as follows:

- `tools/<category>/<tool>` contains the project for `<tool>`. `<category>` is the type of tool; currently this is only `artifact-checkers`, but in the future we could support other types of tool.

- `tools/support` contains scripts used to install the tools into the `run` directory of a fallout process.  These are used to provision the tools in `falloutctl`, docker images, and for unit testing and running development servers via `./gradlew runServer`.

# How fallout calls tools

At the moment, only artifact checkers are supported, via [`ToolArtifactChecker`](../src/main/java/com/datastax/fallout/components/tools/ToolArtifactChecker.java).  Extending this support to other components would require making that component implement [`ToolComponent`](../src/main/java/com/datastax/fallout/components/tools/ToolComponent.java): initialising the component with a [`ToolExecutor`](../src/main/java/com/datastax/fallout/components/tools/ToolExecutor.java) happens automatically within [`ActiveTestRunBuilder`](../src/main/java/com/datastax/fallout/harness/ActiveTestRunBuilder.java).

The [`ToolExecutor`](../src/main/java/com/datastax/fallout/components/tools/ToolExecutor.java) takes care of finding and executing a specific tool within the `run` hierarchy that `tools/support` installs the tools into.  To mitigate pollution of the filesystem, since tools are run on the main server, `ToolExecutor` sets the working directory for the tool to the artifact path of the TestRun.

# Writing new tools

At the moment, only python3-based tools packaged using [`poetry`](python-poetry.org) are supported; see the [`no-op`](artifact-checkers/no-op) tool for an example.

It should be possible to add a new tool by copying the example into a new directory and renaming no-op/no_op to the desired tool name.  Once that's been done, the `build.gradle` file needs to be linked into the main build by editing [`settings.gradle`](../settings.gradle).
