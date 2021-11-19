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
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import static com.google.common.io.Files.getFileExtension;
import static com.google.common.io.Files.getNameWithoutExtension;

public class FileUtils
{
    public static void compressGZIP(Path input, Path output) throws IOException
    {
        try (GZIPOutputStream out = new GZIPOutputStream(Files.newOutputStream(output)))
        {
            Files.copy(input, out);
        }
    }

    public static void uncompressGZIP(Path input, Path output) throws IOException
    {
        try (GZIPInputStream in = new GZIPInputStream(Files.newInputStream(input)))
        {
            Files.copy(in, output);
        }
    }

    public static String readFileFromZipAsString(Path zipFilePath, String fileName) throws IOException
    {
        ZipFile zipFile = new ZipFile(zipFilePath.toFile());
        Enumeration<? extends ZipEntry> entries = zipFile.entries();
        List<String> seenEntries = new ArrayList<>();
        while (entries.hasMoreElements())
        {
            ZipEntry entry = entries.nextElement();
            if (entry.getName().equals(fileName))
            {
                try (InputStream stream = zipFile.getInputStream(entry))
                {
                    return readString(stream);
                }
            }
            seenEntries.add(entry.getName());
        }
        throw new IOException(
            fileName + " was not found in zip file " + zipFilePath + " with contents:\n" + String.join("\n",
                seenEntries));
    }

    public static Path createDirs(Path dir)
    {
        return Exceptions.getUncheckedIO(() -> Files.createDirectories(dir)
        );
    }

    public static void deleteDir(Path dir)
    {
        Exceptions.runUncheckedIO(() -> org.apache.commons.io.FileUtils.deleteDirectory(dir.toFile())
        );
    }

    public static List<Path> listDir(Path dir)
    {
        try (Stream<Path> paths = Exceptions.getUncheckedIO(() -> Files.list(dir)))
        {
            // finalize stream so that we can close the open directory
            return paths.collect(Collectors.toList());
        }
    }

    public static void writeString(Path file, String content)
    {
        Exceptions.runUncheckedIO(() -> Files.writeString(file, content));
    }

    public static String readString(Path file)
    {
        return Exceptions.getUncheckedIO(() -> Files.readString(file));
    }

    public static String readString(InputStream inputStream)
    {
        return Exceptions.getUncheckedIO(() -> new String(inputStream.readAllBytes(), StandardCharsets.UTF_8));
    }

    public interface FileContentsConsumer<R>
    {
        /** Process the contents of a file at filePath, optionally with an archivePath if the filePath
         *  is an archive */
        R apply(Path filePath, Optional<String> archivePath, InputStream contents);

        default R apply(Path filePath, InputStream contents)
        {
            return apply(filePath, Optional.empty(), contents);
        }
    }

    public static <R> List<R> withZippedFileContents(Path zipFilePath, FileContentsConsumer<R> consumer)
    {
        return Exceptions.getUncheckedIO(() -> {
            try (var zipFile = new ZipFile(zipFilePath.toFile(), StandardCharsets.UTF_8))
            {
                return zipFile.stream()
                    .filter(entry -> !entry.isDirectory())
                    .map(entry -> Exceptions.getUncheckedIO(() -> {
                        try (var inputStream = zipFile.getInputStream(entry))
                        {
                            return consumer.apply(zipFilePath, Optional.of(entry.getName()), inputStream);
                        }
                    }))
                    .collect(Collectors.toList());
            }
        });
    }

    public static <R> R withFileContent(Path filePath, FileContentsConsumer<R> consumer)
    {
        return Exceptions.getUncheckedIO(() -> {
            try (var inputStream = Files.newInputStream(filePath))
            {
                return consumer.apply(filePath, inputStream);
            }
        });
    }

    public static <R> R withGzippedFileContent(Path gzipFilePath, FileContentsConsumer<R> consumer)
    {
        return withFileContent(gzipFilePath, (fileName, archivePath, inputStream) -> {
            return Exceptions.getUncheckedIO(() -> {
                try (var gzipInputStream = new GZIPInputStream(inputStream))
                {
                    return consumer.apply(gzipFilePath, gzipInputStream);
                }
            });
        });
    }

    /** Return whether path has the extension _and_ if the path without that extension is accepted
     *  by matches */
    private static boolean matchesPathWithoutExtension(Predicate<Path> matches, Path path, String extension)
    {
        final var pathExtension = getFileExtension(path.toString()).toLowerCase(Locale.ROOT);

        return pathExtension.equals(extension) &&
            matches.test(
                path.getParent().resolve(getNameWithoutExtension(path.getFileName().toString())));
    }

    /** Recurse rootPath.  If a path (with zip and gz extensions removed) satisfies consume, then the path
     *  (with no extensions removed) is passed to consumer with a stream of its uncompressed contents.   Returns
     *  whether any files were found. */
    public static <R> List<R> withMaybeCompressedFileContentsInPath(Path rootPath,
        Predicate<Path> consume,
        FileContentsConsumer<R> consumer)
    {
        return Exceptions.getUncheckedIO(() -> {
            try (Stream<Path> pathStream = Files.walk(rootPath))
            {
                return pathStream
                    .filter(path -> !Files.isDirectory(path))
                    .flatMap(path -> {
                        if (matchesPathWithoutExtension(consume, path, "zip"))
                        {
                            return withZippedFileContents(path, consumer).stream();
                        }
                        else if (matchesPathWithoutExtension(consume, path, "gz"))
                        {
                            return Stream.of(withGzippedFileContent(path, consumer));
                        }
                        else if (consume.test(path))
                        {
                            return Stream.of(withFileContent(path, consumer));
                        }
                        return Stream.empty();
                    })
                    .collect(Collectors.toList());
            }
        });
    }
}
