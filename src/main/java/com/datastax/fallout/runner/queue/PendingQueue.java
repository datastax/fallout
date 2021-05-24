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
package com.datastax.fallout.runner.queue;

import java.util.Collection;

import com.datastax.fallout.service.core.TestRun;

/**
 * Abstracts the means by which {@link TestRunQueue} gets a list of tests for processing.
 *
 * A PendingQueue is assumed to include all tests that are eligible for running; this may overlap with
 * the set of tests that are currently being processed by TestRunnerJobQueue.
 */
public interface PendingQueue
{
    void add(TestRun testRun);

    /** View the list of pending {@link TestRun}s for processing. */
    Collection<TestRun> pending();

    /** Remove testRun if present, and return true if it was */
    boolean remove(TestRun testRun);
}
