package com.datastax.fallout.gradle.fork

import org.gradle.api.Plugin
import org.gradle.api.Project
import org.gradle.api.internal.file.FileResolver
import org.gradle.kotlin.dsl.create
import javax.inject.Inject

/** Creates the [ForkActionFactory] extension that's needed by the [Fork] task */
class ForkPlugin @Inject constructor(private val fileResolver: FileResolver) : Plugin<Project> {
    override fun apply(project: Project) {
        project.extensions.create<ForkActionFactory>("fork", fileResolver)
    }
}
