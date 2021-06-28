/** Adds a lint task if one doesn't already exist, makes it depend on any `compile*` tasks,
 *  and if applied to a root project, on any lint tasks in subprojects and includedBuilds */

package com.datastax.fallout.conventions

import com.datastax.fallout.gradle.common.cascadeTask

val lint by tasks.registering

project.afterEvaluate {
    lint.configure {
        dependsOn(tasks.matching { it.name.startsWith("compile") })
    }
}

cascadeTask(project, lint)
