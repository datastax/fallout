package com.datastax.fallout.gradle.conventions.common

import org.gradle.api.Task
import org.gradle.api.UnknownTaskException
import org.gradle.api.tasks.TaskContainer
import org.gradle.api.tasks.TaskProvider
import org.gradle.kotlin.dsl.*

inline fun <reified T : Task> TaskContainer.maybeRegister(taskName: String): TaskProvider<T> =
    try {
        named<T>(taskName)
    } catch (ex: UnknownTaskException) {
        register<T>(taskName)
    }

inline fun <reified T : Task> TaskContainer.maybeRegister(taskName: String, noinline configuration: T.() -> Unit):
    TaskProvider<T> =
    this.maybeRegister<T>(taskName).apply {
        configure(configuration)
    }
