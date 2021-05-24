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
package com.datastax.fallout.harness;

import clojure.lang.APersistentMap;
import jepsen.client.Client;

import static com.datastax.fallout.harness.JepsenApi.END;
import static com.datastax.fallout.harness.JepsenApi.MODULE;
import static com.datastax.fallout.harness.JepsenApi.TYPE;

class AbortableModule implements Client
{
    private final TestRunAbortedStatus testRunAbortedStatus;
    private final Module module;
    private boolean setup;

    AbortableModule(TestRunAbortedStatus testRunAbortedStatus, Module module)
    {
        this.testRunAbortedStatus = testRunAbortedStatus;
        this.module = module;
        setup = false;
        module.setTestRunAbortedCheck(() -> testRunAbortedStatus.hasBeenAborted());
    }

    @Override
    public Object setup_BANG_(Object test, Object node)
    {
        if (!testRunAbortedStatus.hasBeenAborted())
        {
            module.setup_BANG_(test, node);
            setup = true;
        }

        return this;
    }

    @Override
    public Object invoke_BANG_(Object test, Object op)
    {
        if (!testRunAbortedStatus.hasBeenAborted())
        {
            return module.invoke_BANG_(test, op);
        }

        return ((APersistentMap) op)
            .assoc(TYPE, END)
            .assoc(MODULE, module);
    }

    @Override
    public Object teardown_BANG_(Object test)
    {
        if (setup)
        {
            module.teardown_BANG_(test);
            setup = false;
        }
        return this;
    }
}
