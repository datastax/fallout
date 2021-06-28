package com.datastax.fallout.gradle.common

import org.gradle.api.Project
import org.gradle.api.Task
import org.gradle.api.tasks.TaskProvider
import org.slf4j.LoggerFactory

val logger = LoggerFactory.getLogger("com.datastax.fallout.gradle.common.CascadeTask")

/** Cascade a tasks dependencies down from the root to all
 *  subprojects and includedBuilds, iff project is a root project.
 *
 *  Cascade the named task to subprojects: normally, we wouldn't need this since `./gradlew <task>` runs
 *  `<task>` in all subprojects.  However, when a build is included via `includedBuilds`,
 *  we can't refer to a non-explicit task in the includedBuild, either in the
 *  including build or on the command line.  By using [cascadeTask], we can
 *
 *  - make it possible for an explicit task name to cascade to subprojects,
 *    so we only need to name the task name in the top-level includedBuild
 *  - make it possible for an including build to trivially invoke the same target on all included builds
 *
 *  This means that if we call, say, `cascadeTask(project, tasks.named("clean")`, then invoking `./gradlew clean` on
 *  the command line will clean all included builds as well as subprojects, and if the same `cascadeTask` call was
 *  made in the includedBuild, we can be sure that clean will be invoked in all the included build's subprojects.
 */
fun <T : Task> cascadeTask(project: Project, task: TaskProvider<T>) {
    if (project.rootProject === project) {
        task.configure {
            logger.info("Cascading :${project.name}:${task.name}:")

            project.subprojects {
                tasks.maybeNamed<Task>(task.name)?.let {
                    logger.info("  -> subproject :${this.name}:${it.name}")
                    dependsOn(it)
                }
            }

            project.gradle.includedBuilds.forEach { includedBuild ->
                logger.info("  -> includedBuild :${includedBuild.name}:${task.name}")
                dependsOn(includedBuild.task(":${task.name}"))
            }
        }
    }
}
