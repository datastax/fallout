/*
 * Copyright 2021 DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.fallout.util;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Optional;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.fallout.TestHelpers;

import static com.datastax.fallout.assertj.Assertions.assertThat;

public class FileUtilsTest extends TestHelpers.ArtifactTest
{
    private static final Logger classLogger = LoggerFactory.getLogger(FileUtilsTest.class);

    @Test
    public void testCompressUncompressCycle() throws IOException
    {
        Path uncompressed = createTestRunArtifact("foo.log", "Hello\n");

        Path compressed = uncompressed.resolveSibling(uncompressed.getFileName() + ".gz");
        FileUtils.compressGZIP(uncompressed, compressed);
        assertThat(uncompressed).exists();
        assertThat(compressed).exists();

        Path newUncompressed = uncompressed.resolveSibling("bar.log");
        FileUtils.uncompressGZIP(compressed, newUncompressed);
        assertThat(compressed).exists();
        assertThat(newUncompressed).exists();
        assertThat(newUncompressed).hasContent("Hello\n");
    }

    @Test
    public void testReadFileFromZipAsString() throws IOException
    {
        // file created with the following commands:
        // echo "foo\nbar" > bar.txt; zip foo.zip bar.txt
        String content = FileUtils.readFileFromZipAsString(getTestClassResourceAsPath("foo.zip"), "bar.txt");
        assertThat(content).isEqualTo("foo\nbar\n");
    }

    public record FileContents(Path path, Optional<String> archivePath, String content) {
        static FileContents of(Path path, Optional<String> archivePath, String content)
        {
            return new FileContents(path, archivePath, content);
        }

        static FileContents of(Path path, String archivePath, String content)
        {
            return of(path, Optional.of(archivePath), content);
        }

        static FileContents of(Path path, String content)
        {
            return of(path, Optional.empty(), content);
        }

        static FileUtils.FileContentsConsumer<FileContents> CONSUMER =
            (filePath, archivePath, contents) -> FileContents.of(filePath, archivePath, FileUtils.readString(contents));
    }

    @Test
    public void withFileContent_reads_content()
    {
        final var systemLogPath = getTestClassResourceAsPath("system-logs/system.log");
        assertThat(FileUtils.withFileContent(systemLogPath, FileContents.CONSUMER))
            .isEqualTo(FileContents.of(systemLogPath, "This is the content of system.log\n"));
    }

    @Test
    public void withGZippedFileContent_reads_content()
    {
        final var systemLogGzPath = getTestClassResourceAsPath("system-logs/system.log.gz");
        assertThat(FileUtils.withGzippedFileContent(systemLogGzPath, FileContents.CONSUMER))
            .isEqualTo(FileContents.of(systemLogGzPath, "This is the content of system.log\n"));
    }

    @Test
    public void withZippedFileContents_reads_all_content()
    {
        final var systemLogZipPath = getTestClassResourceAsPath("system-logs/system.log.zip");

        assertThat(FileUtils.withZippedFileContents(systemLogZipPath, FileContents.CONSUMER))
            .containsExactlyInAnyOrder(
                FileContents.of(systemLogZipPath, "system.log.1",
                    "This is the content of system.log.1\n"),
                FileContents.of(systemLogZipPath, "system.log.2",
                    "This is the content of system.log.2\n"));
    }

    @Test
    public void withMaybeCompressedFileContentsInPath_finds_compressed_and_uncompressed_matching_files()
    {
        final var rootPath = getTestClassResourceAsPath("system-logs");

        assertThat(FileUtils.withMaybeCompressedFileContentsInPath(
            rootPath,
            path -> path.getFileName().toString().contains("system.log"), FileContents.CONSUMER))
                .containsExactlyInAnyOrder(
                    FileContents.of(rootPath.resolve("system.log"), "This is the content of system.log\n"),
                    FileContents.of(rootPath.resolve("system.log.1"), "This is the content of system.log.1\n"),
                    FileContents.of(rootPath.resolve("system.log.2"), "This is the content of system.log.2\n"),
                    FileContents.of(rootPath.resolve("system.log.gz"), "This is the content of system.log\n"),
                    FileContents.of(rootPath.resolve("system.log.zip"), "system.log.1",
                        "This is the content of system.log.1\n"),
                    FileContents.of(rootPath.resolve("system.log.zip"), "system.log.2",
                        "This is the content of system.log.2\n")
                );
    }
}
