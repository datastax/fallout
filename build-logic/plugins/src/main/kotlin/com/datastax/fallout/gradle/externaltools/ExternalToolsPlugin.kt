package com.datastax.fallout.gradle.externaltools

import com.google.gradle.osdetector.OsDetectorPlugin
import org.gradle.api.Plugin
import org.gradle.api.Project
import org.gradle.kotlin.dsl.create

class ExternalToolsPlugin : Plugin<Project> {
    override fun apply(project: Project) {
        project.pluginManager.apply(OsDetectorPlugin::class.java)

        project.extensions.create<ExternalToolsExtension>(
            "externalTools",
            project
        )
    }
}
