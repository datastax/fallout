/*
 * Copyright 2020 DataStax, Inc.
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
package com.datastax.fallout.ops;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.PosixFilePermissions;

import org.apache.commons.io.FileUtils;

import com.datastax.fallout.util.Exceptions;

public class LocalScratchSpace implements AutoCloseable
{
    protected static final FileAttribute DEFAULT_TEMPORARY_FILE_ATTRIBUTES =
        PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rwx------"));

    private Path tempDir;

    public LocalScratchSpace(String prefix)
    {
        createTemporaryDirectory(prefix);
    }

    private void createTemporaryDirectory(String prefix)
    {
        Exceptions.getUncheckedIO(() -> {
            tempDir = Files.createTempDirectory(prefix, DEFAULT_TEMPORARY_FILE_ATTRIBUTES);
            return tempDir;
        });
    }

    private void deleteTemporaryDirectory()
    {
        Exceptions.runUncheckedIO(() -> FileUtils.deleteDirectory(tempDir.toFile()));
    }

    public Path createFile(String prefix, String suffix)
    {
        return Exceptions
            .getUncheckedIO(() -> Files.createTempFile(tempDir, prefix, suffix, DEFAULT_TEMPORARY_FILE_ATTRIBUTES));
    }

    public Path createDirectory(String prefix)
    {
        return Exceptions
            .getUncheckedIO(() -> Files.createTempDirectory(tempDir, prefix, DEFAULT_TEMPORARY_FILE_ATTRIBUTES));
    }

    @Override
    public void close()
    {
        deleteTemporaryDirectory();
    }
}
