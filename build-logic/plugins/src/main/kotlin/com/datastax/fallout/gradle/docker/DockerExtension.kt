package com.datastax.fallout.gradle.docker

import org.gradle.api.Project
import org.gradle.kotlin.dsl.*

abstract class DockerExtension(project: Project) {
    /** [org.gradle.api.file.CopySpec] specifying extra files to be included in `$contextDir/image-files` */
    val imageFiles = project.copySpec()

    /** [org.gradle.api.file.CopySpec] specifying extra files to be included in `$contextDir/build-files` */
    val buildFiles = project.copySpec()

    /** The path of a pip.conf file to be copied by `dockerPreparePipConf`;
     *  defaults to the gradle property `dockerPipConfiPath` */
    val pipConfPath = project.objects.fileProperty()

    val dockerSourceDir = project.layout.projectDirectory.dir("docker")
    val contextDir = project.layout.buildDirectory.dir("docker-context")

    /** The name part of the tag that the `dockerBuild` task will use to tag the image as `<name>:<version>` */
    val tagName = project.objects.property<String>()

    /** The version part of the tag that the `dockerBuild` task will use to tag the image as `<name>:<version>` */
    val tagVersion = project.objects.property<String>()

    val latestTag = tagName.map { "$it:latest" }
    val versionTag = tagName.flatMap { name -> tagVersion.map { version -> "$name:$version" } }

    /** The registry that the `dockerPush` task will push to; defaults to
     *  the gradle property `dockerRegistry` or, if unset, `docker.io` */
    val registry = project.objects.property<String>()

    val composeFile = dockerSourceDir.file("docker-compose.yml")
    val composeEnvironment = mutableMapOf<String, Any>()

    init {
        (project.findProperty("dockerPipConfPath") as String?)?.let { pipConfPath ->
            // Note that this correctly handles an absolute pipConfPath
            this.pipConfPath.convention(project.layout.projectDirectory.file(pipConfPath))
        }
        registry.convention(project.findProperty("dockerRegistry") as String? ?: "docker.io")
    }
}
