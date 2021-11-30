package com.datastax.fallout.gradle.git

import org.gradle.api.Project
import org.gradle.api.provider.Property
import java.io.ByteArrayOutputStream
import java.nio.charset.StandardCharsets
import kotlin.reflect.KProperty

abstract class GitExtension(private val project: Project) {

    /** The pattern used to filter gitDescribe results; must be set */
    abstract val describeTagPattern: Property<String>

    /** Return a sensible execution environment for git
     *
     * The gradle daemon doesn't unset environment variables, it just sets them
     * to empty.  If we run gradle as part of a `git -x './gradlew compile' rebase`
     * or a bisect, then GIT_WORK_TREE and GIT_DIR will be set; running outside of
     * the bisect or rebase, the two env vars will be set to empty, which will cause
     * git to fail.  In fact, _any_ value of these env vars will interfere with git
     * commands run as part of this build, so we just remove them unconditionally.
     */
    fun createExecEnvironment(currentEnv: Map<String, Any>): Map<String, Any> =
        currentEnv.filter { it.key != "GIT_DIR" && it.key != "GIT_WORK_TREE" }

    fun execGitWithOutput(vararg command: String): String {
        val outputStream = ByteArrayOutputStream()
        val errorStream = ByteArrayOutputStream()

        val execResult = project.exec {
            executable = "git"
            args = listOf(*command)
            standardOutput = outputStream
            errorOutput = errorStream
            isIgnoreExitValue = true
            environment = createExecEnvironment(environment)
        }

        val output = outputStream.toString(StandardCharsets.UTF_8)
        val error = errorStream.toString(StandardCharsets.UTF_8)

        if (execResult.exitValue != 0) {
            throw RuntimeException(
                """
                'git ${command.joinToString(" ")}' failed (exit code ${execResult.exitValue}):
                STDOUT:
                $output
                STDERR:
                $error
                """.trimIndent()
            )
        }

        return output.trim()
    }

    private val gitDir by lazy {
        try {
            execGitWithOutput("rev-parse", "--git-dir")
        } catch (e: RuntimeException) {
            null
        }
    }

    /** Delegate that lazily initializes a git value by:
     *  - using the gradle property `git<ValueName>`, and if that doesn't exist:
     *  - invoking init */
    private inner class OverrideableLazyGit(private val init: (String) -> String) {
        operator fun provideDelegate(thisRef: GitExtension, prop: KProperty<*>):
            Lazy<String> {

            val gradlePropertyName = "git" + prop.name.capitalize()
            return lazy {
                (project.findProperty(gradlePropertyName) as String?)
                ?: gitDir?.let {
                    init.invoke(it)
                }
                ?: "NO_GIT_REPO"
            }
        }
    }

    val describe by OverrideableLazyGit {
        execGitWithOutput("describe", "--tags", "--match", describeTagPattern.get())
    }

    val branch by OverrideableLazyGit {
        execGitWithOutput("rev-parse", "--abbrev-ref", "HEAD")
    }

    val commit by OverrideableLazyGit {
        execGitWithOutput("rev-parse", "HEAD")
    }

    val repo by OverrideableLazyGit {
        execGitWithOutput("remote", "get-url", "origin")
            .replace(Regex("^.*github.com[:/](.*?)(:?\\.git)?$"), "$1")
            .toLowerCase()
    }
}
