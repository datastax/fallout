@file:Suppress("UnstableApiUsage")

package com.datastax.fallout.gradle.symlinks.tasks

import org.gradle.api.DefaultTask
import org.gradle.api.tasks.Input
import org.gradle.api.tasks.OutputFile
import org.gradle.api.tasks.TaskAction
import org.gradle.kotlin.dsl.property
import java.nio.file.Files
import java.nio.file.LinkOption
import java.nio.file.Paths

open class Symlink : DefaultTask() {
    @Input
    val source = project.objects.property<String>()

    @OutputFile
    val dest = project.objects.fileProperty()

    @TaskAction
    fun symlink() {
        val destPath = dest.get().asFile.toPath()
        val sourcePath = Paths.get(source.get())

        if (Files.exists(destPath, LinkOption.NOFOLLOW_LINKS)) {
            if (Files.isSymbolicLink(destPath) && Files.readSymbolicLink(destPath) == sourcePath) {
                project.logger.info("$destPath already points to $sourcePath; skipping")
                return
            }
            project.logger.info("$destPath does not point at $sourcePath; deleting")
            Files.delete(destPath)
        }

        Files.createSymbolicLink(destPath, sourcePath)
    }
}
