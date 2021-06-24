package com.datastax.fallout.gradle.common

import org.gradle.api.DefaultTask
import org.gradle.api.tasks.TaskAction

open class LockDependencies : DefaultTask() {
    @TaskAction
    fun resolve() {
        require(project.gradle.startParameter.isWriteDependencyLocks) {
            "$name must be called with the --write-locks option"
        }
        project.configurations.filter {
            it.isCanBeResolved
        }.forEach { it.resolve() }
    }
}
