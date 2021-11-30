package com.datastax.fallout.gradle.git

import org.gradle.api.Plugin
import org.gradle.api.Project
import org.gradle.kotlin.dsl.*

class GitPlugin : Plugin<Project> {
    override fun apply(project: Project) {
        val git = project.extensions.create<GitExtension>("git")

        project.tasks.register("gitVars") {
            group = "versioning"
            description = "Show the version properties available in the git extension"
            doLast {
                println("""
                    describe: ${git.describe}
                    branch:   ${git.branch}
                    commit:   ${git.commit}
                    repo:     ${git.repo}
                    """.trimIndent()
                )
            }
        }
    }
}
