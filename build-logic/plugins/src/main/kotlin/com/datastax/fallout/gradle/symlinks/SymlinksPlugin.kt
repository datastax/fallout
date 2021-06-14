package com.datastax.fallout.gradle.symlinks

import com.datastax.fallout.gradle.camelCase
import com.datastax.fallout.gradle.symlinks.tasks.CopySymlinks
import com.datastax.fallout.gradle.symlinks.tasks.SyncSymlinks
import org.gradle.api.DefaultTask
import org.gradle.api.Plugin
import org.gradle.api.Project
import org.gradle.api.Task
import org.gradle.api.distribution.Distribution
import org.gradle.api.distribution.DistributionContainer
import org.gradle.api.distribution.plugins.DistributionPlugin
import org.gradle.api.tasks.TaskProvider
import org.gradle.api.tasks.bundling.AbstractArchiveTask
import org.gradle.api.tasks.bundling.Tar
import org.gradle.api.tasks.bundling.Zip
import org.gradle.kotlin.dsl.*

/** Adds new distribution tasks that use the [SyncSymlinks] task, disables
 *  existing distribution tasks and makes them depend on the new ones (because
 *  replacing tasks isn't easy)
 *
 *  This plugin exists to work around https://github.com/gradle/gradle/issues/3982
 */
class SymlinksPlugin : Plugin<Project> {

    companion object {
        private const val DISTRIBUTION_GROUP = "distribution"
        private const val SYMLINK_DIST_TASK_DISCRIMINATOR = "distWithSymlinks"
        private const val DIST_TASK_DISCRIMINATOR = "dist"
    }

    override fun apply(project: Project) {
        project.pluginManager.apply(DistributionPlugin::class.java)

        project.extensions.extraProperties.apply {
            set(SyncSymlinks::class.simpleName!!, SyncSymlinks::class.java)
            set(CopySymlinks::class.simpleName!!, CopySymlinks::class.java)
        }

        addDistributionTasks(
            project,
            project.extensions.getByName<DistributionContainer>("distributions")
        )
    }

    /** Mostly copied from the existing [DistributionPlugin] from [gradle-6.8.3](https://github.com/gradle/gradle/blob/v6.8.3/subprojects/plugins/src/main/java/org/gradle/api/distribution/plugins/DistributionPlugin.java) */
    private fun addDistributionTasks(
        project: Project,
        distributions: DistributionContainer
    ) {
        distributions.all {

            val zipTaskName: (String) -> String
            val tarTaskName: (String) -> String
            val installTaskName: (String) -> String
            val assembleTaskName: (String) -> String

            if (name == DistributionPlugin.MAIN_DISTRIBUTION_NAME) {
                zipTaskName = { camelCase(it, "zip") }
                tarTaskName = { camelCase(it, "tar") }
                installTaskName = { camelCase("install", it) }
                assembleTaskName = { camelCase("assemble", it) }
            } else {
                zipTaskName = { camelCase(name, it, "zip") }
                tarTaskName = { camelCase(name, it, "tar") }
                installTaskName = { camelCase("install", name, it) }
                assembleTaskName = { camelCase("assemble", name, it) }
            }

            val zipTask =
                addArchiveTask<Zip>(
                    project,
                    zipTaskName(SYMLINK_DIST_TASK_DISCRIMINATOR), this
                )
            val tarTask =
                addArchiveTask<Tar>(
                    project,
                    tarTaskName(SYMLINK_DIST_TASK_DISCRIMINATOR), this
                )
            val installTask =
                addInstallTask(
                    project,
                    installTaskName(SYMLINK_DIST_TASK_DISCRIMINATOR), this
                )

            addAssembleTask(
                project,
                assembleTaskName(SYMLINK_DIST_TASK_DISCRIMINATOR),
                this,
                zipTask,
                tarTask,
                installTask
            )

            // Disable the existing distribution tasks, and make them depend on the new
            // tasks (disabled tasks will be skipped but their dependencies will still run)
            listOf(zipTaskName, tarTaskName, installTaskName, assembleTaskName)
                .forEach { taskName ->
                    project.tasks.named(taskName(DIST_TASK_DISCRIMINATOR))
                        .configure {
                            dependsOn(taskName(SYMLINK_DIST_TASK_DISCRIMINATOR))
                            enabled = false
                        }
                }
        }
    }

    private inline fun <reified T : AbstractArchiveTask> addArchiveTask(
        project: Project,
        taskName: String,
        distribution: Distribution
    ) = project.tasks.register<T>(taskName) {

        description = "Bundles the project as a distribution."
        group = DISTRIBUTION_GROUP
        archiveBaseName
            .convention(distribution.distributionBaseName)

        val childSpec = project.copySpec()
        childSpec.with(distribution.contents)
        childSpec.into(archiveFileName.flatMap { fileName ->
            archiveExtension.map { extension ->
                fileName.removeSuffix("." + extension)
            }
        })

        with(childSpec)
    }

    private fun addInstallTask(
        project: Project,
        taskName: String,
        distribution: Distribution
    ) = project.tasks.register<SyncSymlinks>(taskName) {

        description = "Installs the project as a distribution as-is."
        group = DISTRIBUTION_GROUP
        with(distribution.contents)
        into(
            project.layout.buildDirectory.dir(
                distribution.distributionBaseName.map { baseName: String -> "install/$baseName" }
            )
        )
    }

    private fun addAssembleTask(
        project: Project,
        taskName: String,
        distribution: Distribution,
        vararg tasks: TaskProvider<out Task>
    ) {
        project.tasks.register<DefaultTask>(taskName) {
            description =
                "Assembles the " + distribution.name + " distributions"
            group = DISTRIBUTION_GROUP
            dependsOn(*tasks)
        }
    }
}
