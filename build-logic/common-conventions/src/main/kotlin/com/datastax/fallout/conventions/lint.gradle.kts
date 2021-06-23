/** Adds a lint task if one doesn't already exist, makes it depend on any `compile*` tasks,
 *  and if applied to a root project, on any lint tasks in subprojects and includedBuilds */

package com.datastax.fallout.conventions

import com.datastax.fallout.gradle.common.maybeNamed

val lint = tasks.register("lint")

project.afterEvaluate {
    lint.configure {
        dependsOn(tasks.matching { it.name.startsWith("compile") })
    }
}

// If this is the root project, cascade the lint task to all subprojects and included builds
if (project.rootProject === project) {
    lint.configure {
        subprojects {
            tasks.maybeNamed<Task>("lint")?.let {
                dependsOn(it)
            }
        }

        gradle.includedBuilds.forEach { includedBuild ->
            dependsOn(includedBuild.task(":lint"))
        }
    }
}
