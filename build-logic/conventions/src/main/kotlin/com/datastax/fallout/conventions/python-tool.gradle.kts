package com.datastax.fallout.conventions

import org.gradle.api.tasks.Exec

project.plugins.apply("com.datastax.fallout.conventions.python")

val toolName = project.name
val toolCategory = project.parent!!.name
val toolContents = project.fileTree(project.projectDir) {
    exclude(project.buildDir.name)
    include(
        "src/**/*.py",
        "poetry.lock",
        "pyproject.toml"
    )
}

with(project.extra) {
    set("isPythonTool", true)
    set("toolName", toolName)
    set("toolCategory", toolCategory)
    set("toolContents", toolContents)
}

project.tasks {
    register<Exec>("installToolForTesting") {
        inputs.files(toolContents)

        val testToolsRunDir = project.rootProject.extra.get("testToolsRunDir")
        outputs.file("$testToolsRunDir/tools/$toolCategory/venvs/$toolName/bin/$toolName")
        outputs.file("$testToolsRunDir/tools/$toolCategory/bin/$toolName")

        commandLine(
            "${project.rootProject.projectDir}/tools/support/install-python-tool",
            project.projectDir, testToolsRunDir
        )
    }
}
