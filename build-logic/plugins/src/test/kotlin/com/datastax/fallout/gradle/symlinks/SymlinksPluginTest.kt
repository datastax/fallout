package com.datastax.fallout.gradle.symlinks

import org.assertj.core.api.AbstractPathAssert
import org.assertj.core.api.Assertions.assertThat
import org.gradle.testkit.runner.GradleRunner
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import java.io.File
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardOpenOption

class SymlinksPluginTest {

    private lateinit var testProjectDir: Path
    private lateinit var settingsFile: Path
    private lateinit var buildFile: Path
    private lateinit var sourceDir: Path
    private lateinit var destDir: Path
    private lateinit var realFile: Path
    private lateinit var realFileContent: String
    private lateinit var symLink: Path

    @BeforeEach
    fun setup(@TempDir testProjectDir_: File) {
        testProjectDir = testProjectDir_.toPath()

        settingsFile = testProjectDir.resolve("settings.gradle.kts")
        Files.writeString(
            settingsFile,
            """
                rootProject.name = "test"
            """
        )

        buildFile = testProjectDir.resolve("build.gradle.kts")

        Files.writeString(
            buildFile,
            """
            plugins {
                id("com.datastax.fallout.symlinks")
            }
            """.trimIndent()
        )

        sourceDir = testProjectDir.resolve("source")
        Files.createDirectories(sourceDir)

        destDir = testProjectDir.resolve("dest")
        assertThat(destDir).doesNotExist()

        realFile = sourceDir.resolve("real-file")
        realFileContent = "a real file"
        Files.writeString(realFile, realFileContent)

        symLink = sourceDir.resolve("symlink")
        Files.createSymbolicLink(symLink, sourceDir.relativize(realFile))
        assertThat(symLink)
            .hasContent(realFileContent)
            .isSymbolicLink()
    }

    private fun assertDirContainsRealFileAndSymLink(destDir: Path): AbstractPathAssert<*>? {
        assertThat(destDir.resolve(sourceDir.relativize(realFile)))
            .exists()
            .hasContent(realFileContent)
            .isRegularFile()

        assertThat(destDir.resolve(sourceDir.relativize(symLink)))
            .exists()
            .hasContent(realFileContent)
            .isSymbolicLink()

        return assertThat(
            Files.readSymbolicLink(
                destDir.resolve(sourceDir.relativize(symLink))
            )
        )
            .isRelative()
            .isEqualTo(sourceDir.relativize(realFile))
    }

    fun runTask(task: String) {
        GradleRunner.create()
            .withProjectDir(testProjectDir.toFile())
            .withArguments(task, "--stacktrace")
            .withPluginClasspath()
            .build()
    }

    @Test
    fun `correctly copies symlinks`() {
        Files.writeString(
            buildFile,
            """
            import com.datastax.fallout.gradle.symlinks.tasks.CopySymlinks
            
            tasks.register<CopySymlinks>("copySymlinks") {
                from("source")
                into("dest")
            }
            """.trimIndent(),
            StandardOpenOption.APPEND
        )

        runTask("copySymlinks")

        assertDirContainsRealFileAndSymLink(destDir)

        // Check that deleted files in the source persist in the destination
        Files.delete(symLink)

        runTask("copySymlinks")

        assertDirContainsRealFileAndSymLink(destDir)
    }

    @Test
    fun `correctly syncs symlinks`() {
        Files.writeString(
            buildFile,
            """
            import com.datastax.fallout.gradle.symlinks.tasks.SyncSymlinks
            
            tasks.register<SyncSymlinks>("syncSymlinks") {
                from("source")
                into("dest")
            }
            """.trimIndent(),
            StandardOpenOption.APPEND
        )

        runTask("syncSymlinks")

        assertDirContainsRealFileAndSymLink(destDir)

        // Check that deleted files in the source are deleted in the destination
        Files.delete(symLink)

        runTask("syncSymlinks")

        assertThat(destDir.resolve(sourceDir.relativize(realFile)))
            .exists()
            .hasContent(realFileContent)
            .isRegularFile()

        assertThat(destDir.resolve(sourceDir.relativize(symLink)))
            .doesNotExist()
    }

    @ParameterizedTest
    @ValueSource(strings = ["installDistWithSymlinks", "installDist"])
    fun `preserves symlinks in distributions`(task: String) {
        Files.writeString(
            buildFile,
            """
            distributions {
                main {
                    contents {
                        from("source")
                    }
                }
            }
            """.trimIndent(),
            StandardOpenOption.APPEND
        )

        runTask(task)

        assertDirContainsRealFileAndSymLink(
            testProjectDir.resolve("build/install/test")
        )
    }
}
