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
package com.datastax.fallout.runner;

import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

import com.datastax.fallout.service.core.TestRun;

/** Handles atomic modifications and persistence to {@link TestRun} */
public class AtomicallyPersistedTestRun
{
    private final TestRun testRun;
    private final Consumer<TestRun> persist;

    /** Wraps the {@link TestRun} with a lock, and calls persist after any modifications */
    public AtomicallyPersistedTestRun(TestRun testRun, Consumer<TestRun> persist)
    {
        this.testRun = testRun;
        this.persist = persist;
    }

    /** Unconditionally call modifier then persist */
    public void update(Consumer<TestRun> modifier)
    {
        updateIfModified(testRun -> {
            modifier.accept(testRun);
            return true;
        });
    }

    /** Call modifier, and if it returns true, call persist */
    public boolean updateIfModified(Predicate<TestRun> modifier)
    {
        synchronized (testRun)
        {
            final boolean modified = modifier.test(testRun);
            if (modified)
            {
                persist.accept(testRun);
            }
            return modified;
        }
    }

    /** Call reader; does not call persist */
    public <T> T get(Function<TestRun, T> reader)
    {
        synchronized (testRun)
        {
            return reader.apply(testRun);
        }
    }
}
