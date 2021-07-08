package com.datastax.fallout.gradle.symlinks

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
    private lateinit var symlinkToRealFile: Path
    private lateinit var subDir: Path
    private lateinit var realFileInSubDir: Path
    private lateinit var realFileInSubDirContent: String
    private lateinit var symlinkToRealFileInSubDir: Path

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

        symlinkToRealFile = sourceDir.resolve("symlink-to-real-file")
        Files.createSymbolicLink(symlinkToRealFile, sourceDir.relativize(realFile))
        assertThat(symlinkToRealFile)
            .hasContent(realFileContent)
            .isSymbolicLink()

        subDir = testProjectDir.resolve("source/sub")
        Files.createDirectories(subDir)

        realFileInSubDir = subDir.resolve("real-file")
        realFileInSubDirContent = "a real file in a subdirectory"
        Files.writeString(realFileInSubDir, realFileInSubDirContent)

        symlinkToRealFileInSubDir = sourceDir.resolve("symlink-to-real-file-in-sub")
        Files.createSymbolicLink(symlinkToRealFileInSubDir, sourceDir.relativize(realFileInSubDir))
        assertThat(symlinkToRealFileInSubDir)
            .hasContent(realFileInSubDirContent)
            .isSymbolicLink()
    }

    private fun assertPathIsSymlinkTo(destDir: Path, symlink: Path, realFile: Path, realFileContent: String) {
        assertThat(destDir.resolve(sourceDir.relativize(realFile)))
            .exists()
            .hasContent(realFileContent)
            .isRegularFile()

        assertThat(destDir.resolve(sourceDir.relativize(symlink)))
            .exists()
            .hasContent(realFileContent)
            .isSymbolicLink()

        assertThat(
            Files.readSymbolicLink(
                destDir.resolve(sourceDir.relativize(symlink))
            )
        )
            .isRelative()
            .isEqualTo(sourceDir.relativize(realFile))
    }

    private fun assertDirContainsRealFilesAndSymLinks(destDir: Path) {
        assertPathIsSymlinkTo(destDir, symlinkToRealFile, realFile, realFileContent)
        assertPathIsSymlinkTo(destDir, symlinkToRealFileInSubDir, realFileInSubDir, realFileInSubDirContent)
    }

    private fun assertCopiesSymlinksCorrectly(task: String, destDir: Path) {
        runTask(task)

        assertDirContainsRealFilesAndSymLinks(destDir)
    }

    private fun assertHandlesDanglingSymlinks(task: String, destDir: Path) {
        // Check that dangling symlinks in the destination are handled
        listOf(realFile, realFileInSubDir, subDir).forEach {
            Files.delete(destDir.resolve(sourceDir.relativize(it)))
        }

        runTask(task)

        assertDirContainsRealFilesAndSymLinks(destDir)
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

        val task = "copySymlinks"

        assertCopiesSymlinksCorrectly(task, destDir)
        assertHandlesDanglingSymlinks(task, destDir)

        // Check that deleted files in the source persist in the destination
        Files.delete(symlinkToRealFile)

        assertCopiesSymlinksCorrectly(task, destDir)
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

        val task = "syncSymlinks"

        assertCopiesSymlinksCorrectly(task, destDir)
        assertHandlesDanglingSymlinks(task, destDir)

        // Check that deleted files in the source are deleted in the destination
        Files.delete(symlinkToRealFile)

        runTask(task)

        assertThat(destDir.resolve(sourceDir.relativize(realFile)))
            .exists()
            .hasContent(realFileContent)
            .isRegularFile()

        assertThat(destDir.resolve(sourceDir.relativize(symlinkToRealFile)))
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

        testProjectDir.resolve("build/install/test").let {
            assertCopiesSymlinksCorrectly(task, it)
            assertHandlesDanglingSymlinks(task, it)
        }
    }
}
