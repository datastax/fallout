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
package com.datastax.fallout;

import com.google.auto.service.AutoService;
import org.junit.platform.launcher.TestExecutionListener;
import org.junit.platform.launcher.TestPlan;

import com.datastax.fallout.harness.ClojureShutdown;

/** {@link ClojureShutdown#shutdown} can only be called once per-JVM: this ensures that we disable
 * shutdown (because it can be called by multiple {@link com.datastax.fallout.service.FalloutService}
 * instances) until after all tests have run, then reenable and execute it. */
@AutoService(TestExecutionListener.class)
public class ClojureShutdownListener implements TestExecutionListener
{
    @Override
    public void testPlanExecutionStarted(TestPlan testPlan)
    {
        ClojureShutdown.disableShutdown();
    }

    @Override
    public void testPlanExecutionFinished(TestPlan testPlan)
    {
        ClojureShutdown.enableShutdown();
        ClojureShutdown.shutdown();
    }
}
