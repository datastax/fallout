@file:Suppress("UnstableApiUsage")

package com.datastax.fallout.gradle.externaltools

import de.undercouch.gradle.tasks.download.DownloadAction
import de.undercouch.gradle.tasks.download.VerifyAction
import org.gradle.api.DefaultTask
import org.gradle.api.GradleException
import org.gradle.api.model.ObjectFactory
import org.gradle.api.tasks.CacheableTask
import org.gradle.api.tasks.Input
import org.gradle.api.tasks.Optional
import org.gradle.api.tasks.OutputFile
import org.gradle.api.tasks.TaskAction
import org.gradle.kotlin.dsl.property
import java.io.File

/** Variant of [de.undercouch.gradle.tasks.download.Download] that
 *  enables output caching and uses the hash as an up-to-date check */
@CacheableTask
open class DownloadTask : DefaultTask() {
    @Input
    val source = project.objects.property<String>()

    @OutputFile
    val dest = project.objects.fileProperty()

    companion object {
        /** convenience constructor that uses a standard default */
        fun algorithmProperty(objects: ObjectFactory) = objects.property<String>().apply {
            convention("sha-256")
        }
    }

    @Input
    @Optional
    val algorithm = algorithmProperty(project.objects)

    @Input
    @Optional
    val checksum = project.objects.property<String>()

    private val destFile: File
        get() = dest.asFile.get()

    @TaskAction
    fun download() {

        // Don't do skipping in an onlyIf spec, because that will disable build caching
        if (existingFileIsValid()) {
            project.logger.info("Skipping download as existing file '$destFile' has valid checksum")
            return
        }

        if (project.gradle.startParameter.isOffline) {
            throw IllegalStateException("Unable to download file '$destFile' in offline mode.")
        }

        val download = DownloadAction(project)
        download.src(source.get())
        download.dest(destFile)
        download.overwrite(true)
        download.tempAndMove(true)
        download.execute()
        if (verifiable()) {
            verify()
        }
    }

    private fun verifiable(): Boolean = checksum.isPresent

    private fun verify(): Boolean {
        val verify = VerifyAction(project)
        verify.src(destFile)
        verify.algorithm(algorithm.get())
        verify.checksum(checksum.get())
        verify.execute()
        return true
    }

    private fun existingFileIsValid() =
        try {
            verifiable() && destFile.exists() && verify()
        } catch (ex: GradleException) {
            project.logger.info(ex.message)
            false
        }
}
