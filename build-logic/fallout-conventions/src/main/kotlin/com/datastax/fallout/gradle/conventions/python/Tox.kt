package com.datastax.fallout.gradle.conventions.python

import com.datastax.fallout.gradle.conventions.PluginInfo
import org.gradle.api.tasks.Exec
import org.gradle.api.tasks.Input
import org.gradle.kotlin.dsl.*

open class Tox : Exec() {

    @Input
    val python = project.objects.property<String>()

    init {
        description = "Run python tests using tox"
        group = "verification"

        executable = "${PluginInfo.getProjectDir()}/ci-tools/tox-bootstrap"

        project.findProperty("toxArgs")?.let { toxArgs ->
            args(toxArgs.toString().split(" "))
        }
    }

    override fun exec() {
        environment("TOX_BOOTSTRAP_PYTHON", python.get())
        super.exec()
    }
}
