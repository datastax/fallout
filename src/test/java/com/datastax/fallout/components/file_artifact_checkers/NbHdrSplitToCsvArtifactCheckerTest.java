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
package com.datastax.fallout.components.file_artifact_checkers;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPInputStream;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.datastax.fallout.ops.PropertySpec;
import com.datastax.fallout.ops.WritablePropertyGroup;
import com.datastax.fallout.test.utils.WithPersistentTestOutputDir;

import static com.datastax.fallout.assertj.Assertions.assertThat;
import static com.datastax.fallout.assertj.Assertions.assertThatThrownBy;

class NbHdrSplitToCsvArtifactCheckerTest extends WithPersistentTestOutputDir
{
    private NbHdrSplitToCsvArtifactChecker checker;

    @BeforeEach
    public void setup()
    {
        checker = new NbHdrSplitToCsvArtifactChecker();
    }

    @Test
    void should_fail_validation_when_there_is_no_tag()
    {
        assertThatThrownBy(() -> checker.validateProperties(new WritablePropertyGroup(Map.of(
            "fallout.artifact_checkers.hdr_split_to_csv.files", List.of("a.hdr.gz", "b.hdr", "c.foo")
        ))))
            .isInstanceOf(PropertySpec.ValidationException.class)
            .hasMessage("Please specify at least one tag");
    }

    @Test
    void should_fail_validation_when_there_is_no_HDR_histograms()
    {
        assertThatThrownBy(() -> checker.validateProperties(new WritablePropertyGroup(Map.of(
            "fallout.artifact_checkers.hdr_split_to_csv.tags", List.of("foo")
        ))))
            .isInstanceOf(PropertySpec.ValidationException.class)
            .hasMessage("Please specify at least one HDR histogram");
    }

    @Test
    void should_fail_validation_when_there_are_other_than_HDR_histograms()
    {
        assertThatThrownBy(() -> checker.validateProperties(new WritablePropertyGroup(Map.of(
            "fallout.artifact_checkers.hdr_split_to_csv.files", List.of("a.hdr.gz", "b.hdr", "c.foo"),
            "fallout.artifact_checkers.hdr_split_to_csv.tags", List.of("foo")
        ))))
            .isInstanceOf(PropertySpec.ValidationException.class)
            .hasMessage("Found invalid files: [a.hdr.gz, c.foo]");
    }

    @Test
    void should_pass_validation_when_there_are_only_HDR_histograms()
    {
        checker.validateProperties(new WritablePropertyGroup(Map.of(
            "fallout.artifact_checkers.hdr_split_to_csv.files", List.of("a.hdr", "b.hdr", "c.hdr"),
            "fallout.artifact_checkers.hdr_split_to_csv.tags", List.of("foo")
        )));
        // PropertyBasedComponent::validateProperties is a void method that throws exception on failure.
        // So if we reach here, we passed validation.
    }

    @Test
    void should_find_raw_input_files_in_directory() throws IOException
    {
        final File foo = persistentTestOutputDir().resolve("foo.hdr").toFile();
        final File zippedBar = persistentTestOutputDir().resolve("bar.hdr").toFile();
        foo.createNewFile();
        zippedBar.createNewFile();

        assertThat(checker.findFilesToProcess(persistentTestOutputDir(), List.of("foo.hdr", "bar.hdr", "qix.hdr")))
            .hasSize(2)
            .contains(foo, zippedBar);
    }

    @Test
    void should_find_zipped_input_files_in_directory() throws IOException
    {
        final File foo = persistentTestOutputDir().resolve("foo.hdr.gz").toFile();
        final File zippedBar = persistentTestOutputDir().resolve("bar.hdr.gz").toFile();
        foo.createNewFile();
        zippedBar.createNewFile();

        assertThat(checker.findFilesToProcess(persistentTestOutputDir(), List.of("foo.hdr", "bar.hdr", "qix.hdr")))
            .hasSize(2)
            .contains(foo, zippedBar);
    }

    @Test
    void should_split_uncompressed_hdr()
    {
        should_split_hdr_file("nb-histogram-with-tags.hdr");
    }

    @Test
    void should_split_compressed_hdr()
    {
        should_split_hdr_file("nb-histogram-with-tags.hdr.gz");
    }

    private void should_split_hdr_file(String fileName)
    {
        File inputFile = copyTestClassResourceToFile(fileName).toFile();

        final String tag1 = "phase1.block8--read-from-table-one--success";
        final String tag2 = "phase1.block8--read-from-table-one--error";
        final Collection<File> splitHdrs = checker.splitHdrHistogram(
            inputFile,
            List.of(tag1, tag2),
            fileName.endsWith(".gz"));

        // The split HDR files should be named after the tag and the HDR file name
        assertThat(splitHdrs.stream().map(File::getName))
            .hasSize(2)
            .containsOnly(
                tag1 + "." + fileName,
                tag2 + "." + fileName);
    }

    @Test
    void should_convert_uncompressed_file_to_csv()
    {
        String fileNameWithoutExt = "phase1.block8--read-from-table-one--success.nb-histogram-with-tags";
        File tempHistogram = copyTestClassResourceToFile(fileNameWithoutExt + ".hdr").toFile();

        checker.convertToCsv(tempHistogram, false);

        final var actual = persistentTestOutputDir().resolve(fileNameWithoutExt + ".csv");
        final var expected = getTestClassResourceAsPath(fileNameWithoutExt + ".csv");

        assertThat(actual).hasSameBinaryContentAs(expected);
    }

    @Test
    void should_convert_compressed_file_to_compressed_csv() throws URISyntaxException, IOException
    {
        String fileNameWithoutExt = "phase1.block8--read-from-table-one--success.nb-histogram-with-tags";
        File tempHistogram = copyTestClassResourceToFile(fileNameWithoutExt + ".hdr").toFile();

        checker.convertToCsv(tempHistogram, true);

        // Unfortunately, we cannot just compare the two .csv.gz files.
        // The reference one was created with `gzip --keep` and contains the name of the CSV file in its header.
        // The compressed CSV created by the JVM lacks that header, as Java does not allow that.
        // See https://stackoverflow.com/questions/3984927/how-do-i-get-a-filename-of-a-file-inside-a-gzip-in-java
        // So we must decompress the file to compare its actual content.
        try (InputStream expected = getTestClassResourceAsStream(fileNameWithoutExt + ".csv");
            GZIPInputStream actual = new GZIPInputStream(
                new FileInputStream(persistentTestOutputDir().resolve(fileNameWithoutExt + ".csv.gz").toFile())))
        {
            assertThat(actual).hasSameContentAs(expected);
        }
    }
}
