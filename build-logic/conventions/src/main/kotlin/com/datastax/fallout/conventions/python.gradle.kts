package com.datastax.fallout.conventions

import com.datastax.fallout.gradle.conventions.python.Tox
import org.gradle.api.tasks.Delete

val pythonExecutable = "python3.7"

project.extra.set("python", pythonExecutable)

project.tasks {

    val tox by registering(Tox::class) {
        python.set(pythonExecutable)
    }

    register<Tox>("lint") {
        description = "Lint the python project as quietly as possible"

        python.set(pythonExecutable)

        environment("TOX_BOOTSTRAP_QUIET", "1")
        // More `-q`, more quiet:
        args("-q", "-q", "-e", "lint")
    }

    register("check") {
        dependsOn(tox)
    }

    register<Delete>("clean") {
        delete(project.buildDir, project.file(".tox"))
    }
}
