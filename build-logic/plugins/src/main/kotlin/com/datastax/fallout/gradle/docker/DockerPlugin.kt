package com.datastax.fallout.gradle.docker

import com.datastax.fallout.gradle.symlinks.tasks.SyncSymlinks
import org.gradle.api.DefaultTask
import org.gradle.api.Plugin
import org.gradle.api.Project
import org.gradle.api.tasks.Exec
import org.gradle.kotlin.dsl.*
import java.nio.charset.StandardCharsets

class DockerPlugin : Plugin<Project> {
    override fun apply(project: Project) {
        val dockerExtension = project.extensions.create<DockerExtension>("docker", project)
        registerTasks(project, dockerExtension)
    }

    private fun registerTasks(project: Project, docker: DockerExtension) {
        val dockerPrepareImageFiles by project.tasks.registering(SyncSymlinks::class) {
            group = "docker"

            val imageFilesDirName = "image-files"
            val imageFilesSourceDir = docker.dockerSourceDir.dir(imageFilesDirName)
            val imageFilesTargetDir = docker.contextDir.map { it.dir(imageFilesDirName) }

            description = "Sync the files in $imageFilesSourceDir (and any specified by docker.imageFiles) " +
                "to ${imageFilesTargetDir.get()}"

            with(docker.imageFiles)

            from(imageFilesSourceDir)
            into(imageFilesTargetDir)
        }

        val dockerPrepareBuildFiles by project.tasks.registering(SyncSymlinks::class) {
            group = "docker"

            val buildFilesDirName = "build-files"
            val buildFilesSourceDir = docker.dockerSourceDir.dir(buildFilesDirName)
            val buildFilesTargetDir = docker.contextDir.map { it.dir(buildFilesDirName) }

            description = "Sync the files in $buildFilesSourceDir (and any specified by docker.buildFiles) " +
                "to ${buildFilesTargetDir.get()}"

            with(docker.buildFiles)

            from(buildFilesSourceDir)
            into(buildFilesTargetDir)
        }

        val dockerPrepare by project.tasks.registering() {
            group = "docker"

            dependsOn(dockerPrepareImageFiles, dockerPrepareBuildFiles)
        }

        val dockerBuild by project.tasks.registering(Exec::class) {
            group = "docker"

            description = "Build the docker image using ${docker.contextDir.get()} as the working directory"

            dependsOn(dockerPrepare)
            workingDir(docker.contextDir)

            commandLine("docker", "buildx", "build",
                "--file", "build-files/Dockerfile",
                "--tag", docker.latestTag.get(),
                "--tag", docker.versionTag.get(),
                "--progress", "plain",
                "--load",
                ".")
        }

        val dockerLogin by project.tasks.registering(Exec::class) {
            group = "docker"

            val DOCKER_USERNAME_PROPERTY = "dockerUsername"
            val DOCKER_PASSWORD_PROPERTY = "dockerPassword"

            description = "If the gradle properties $DOCKER_USERNAME_PROPERTY and " +
            "$DOCKER_PASSWORD_PROPERTY are set, then use them to login to " +
                "the docker registry"

            val dockerUsername = project.findProperty(DOCKER_USERNAME_PROPERTY) as String? ?: ""
            val dockerPassword = project.findProperty(DOCKER_PASSWORD_PROPERTY) as String? ?: ""

            onlyIf {
                dockerUsername.isNotBlank() && dockerPassword.isNotBlank()
            }

            standardInput = dockerPassword.byteInputStream(StandardCharsets.UTF_8)

            commandLine("docker", "login", "-u",
                dockerUsername, "--password-stdin", docker.registry.get())
        }

        project.tasks.register<DefaultTask>("dockerPush") {
            group = "docker"

            description = "Push the docker image to the docker registry defined by " +
                "the docker.registry setting (${docker.registry.get()})"

            dependsOn(dockerBuild, dockerLogin)

            doLast {
                listOf(docker.latestTag, docker.versionTag).forEach { tag ->
                    val registryTag = "${docker.registry.get()}/${tag.get()}"

                    project.exec {
                        commandLine("docker", "image", "tag", tag.get(), registryTag)
                    }
                    project.exec {
                        commandLine("docker", "image", "push", registryTag)
                    }
                }
            }
        }

        project.tasks.register<Exec>("dockerComposeUp") {
            group = "docker"
            dependsOn(dockerBuild)

            environment(docker.composeEnvironment)
            commandLine("docker-compose", "-f", docker.composeFile, "up", "-d")
        }

        project.tasks.register<Exec>("dockerComposeDown") {
            group = "docker"

            commandLine("docker-compose", "-f", docker.composeFile, "down")
        }
    }
}
