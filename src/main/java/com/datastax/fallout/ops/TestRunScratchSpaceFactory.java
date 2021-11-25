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
package com.datastax.fallout.ops;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.Set;

import com.google.common.io.MoreFiles;
import com.google.common.io.RecursiveDeleteOption;

import com.datastax.fallout.harness.ActiveTestRun;
import com.datastax.fallout.service.core.TestRunIdentifier;
import com.datastax.fallout.util.Exceptions;
import com.datastax.fallout.util.FileUtils;

public class TestRunScratchSpaceFactory
{
    private final Path localScratchSpaceRoot;

    private static final FileAttribute<Set<PosixFilePermission>> DEFAULT_TEMPORARY_FILE_ATTRIBUTES =
        PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rwx------"));

    public TestRunScratchSpaceFactory(Path localScratchSpaceRoot)
    {
        this.localScratchSpaceRoot = localScratchSpaceRoot;
    }

    public TestRunScratchSpace create(TestRunIdentifier testRunIdentifier)
    {
        return new TestRunScratchSpace(testRunIdentifier);
    }

    /** Create a scratch space in <code>tmp</code> under the localScratchSpaceRoot directory; this allows us to
     *  create a single-use scratch space for {@link com.datastax.fallout.service.cli.FalloutServerlessCommand}
     *  implementations. */
    public TestRunScratchSpace createGlobal()
    {
        return new TestRunScratchSpace();
    }

    /** Instances of {@link LocalScratchSpace} represent a scratch directory in which paths can be
     * created using {@link #resolve}.
     *
     * <p>New scratch spaces can be created under a {@link LocalScratchSpace} using {@link #makeScratchSpaceFor}.
     * The root of a {@link LocalScratchSpace} hierarchy is {@link TestRunScratchSpace}, which will
     * delete the entire directory hierarchy when {@link TestRunScratchSpace#close} is called.
     */
    public static class LocalScratchSpace
    {
        private final Path root;

        private LocalScratchSpace(Path parent, String component)
        {
            root = parent.resolve(component);
            FileUtils.createDirs(root);
        }

        private LocalScratchSpace(Path parent, PropertyBasedComponent component)
        {
            this(parent, component.name() + "." + component.getInstanceName());
        }

        public LocalScratchSpace makeScratchSpaceFor(PropertyBasedComponent component)
        {
            return new LocalScratchSpace(root, component);
        }

        public LocalScratchSpace makeScratchSpaceFor(String component)
        {
            return new LocalScratchSpace(root, component);
        }

        public Path getPath()
        {
            return root;
        }

        public Path resolve(String filename)
        {
            return root.resolve(filename);
        }
    }

    /** Specialization of {@link LocalScratchSpace} that is the root of all scratch spaces for an
     * {@link ActiveTestRun}, and deletes its contents at the end of a run via
     * {@link AutoCloseable#close} */
    public class TestRunScratchSpace extends LocalScratchSpace implements AutoCloseable
    {
        private TestRunScratchSpace(TestRunIdentifier testRunIdentifier)
        {
            // Use '+' to join the testRunIdentifier components: `_`, '.' and '-' are all likely to be in use.
            super(localScratchSpaceRoot, String.join("+",
                testRunIdentifier.getTestOwner(),
                testRunIdentifier.getTestName(),
                testRunIdentifier.getTestRunId().toString()));
        }

        private TestRunScratchSpace()
        {
            super(localScratchSpaceRoot, "tmp");
        }

        @Override
        public void close()
        {
            if (Files.exists(getPath()))
            {
                Exceptions.runUncheckedIO(
                    () -> MoreFiles.deleteRecursively(getPath(), RecursiveDeleteOption.ALLOW_INSECURE));
            }
        }

        public LocalScratchSpace makeScratchSpaceForWorkload()
        {
            return new LocalScratchSpace(getPath(), "workload");
        }

        public LocalScratchSpace makeScratchSpaceForNodeGroup(String name)
        {
            return new LocalScratchSpace(getPath(), name);
        }
    }
}
