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
package com.datastax.fallout.components.impl;

import clojure.lang.IPersistentMap;
import clojure.lang.Keyword;
import clojure.lang.PersistentHashMap;
import com.google.auto.service.AutoService;

import com.datastax.fallout.harness.Module;
import com.datastax.fallout.harness.Operation;
import com.datastax.fallout.ops.Ensemble;
import com.datastax.fallout.ops.PropertyGroup;

import static com.datastax.fallout.harness.JepsenApi.TIME;
import static com.datastax.fallout.harness.JepsenApi.TYPE;
import static com.datastax.fallout.harness.JepsenApi.VALUE;

/**
 * Module for testing - intentionally emits a history that isn't linearizable.
 */
@AutoService(Module.class)
public class NonlinearizableModule extends Module
{
    private static final Keyword LWTPROCESS = Keyword.intern("lwtprocess");

    @Override
    public String prefix()
    {
        return "test.module.nonlinearizable.";
    }

    @Override
    public String name()
    {
        return "nonlinearizable";
    }

    @Override
    public String description()
    {
        return "A module that emits a nonlinearizable history";
    }

    @Override
    public void run(Ensemble ensemble, PropertyGroup properties)
    {
        IPersistentMap writeInvoke =
            PersistentHashMap.EMPTY.assoc(TYPE, Keyword.intern(Operation.Type.invoke.toString().toLowerCase()))
                .assoc(TIME, 0L)
                .assoc(VALUE, 0)
                .assoc(LWTPROCESS, 1)
                .assoc(Keyword.intern("f"), Keyword.intern("write"));

        IPersistentMap writeOK = writeInvoke.assoc(TIME, 1L)
            .assoc(TYPE, Keyword.intern(Operation.Type.ok.toString().toLowerCase()));

        IPersistentMap firstReadInvoke = writeInvoke.assoc(TIME, 2L)
            .assoc(Keyword.intern("f"), Keyword.intern("read"))
            .without(VALUE);

        IPersistentMap firstReadOK = firstReadInvoke
            .assoc(TYPE, Keyword.intern(Operation.Type.ok.toString().toLowerCase()))
            .assoc(TIME, 3L).assoc(VALUE, 0);

        IPersistentMap secondReadInvoke = firstReadInvoke.assoc(TIME, 4L);

        IPersistentMap secondReadOK = firstReadOK.assoc(TIME, 5l).assoc(VALUE, 1);

        emit(writeInvoke);
        emit(writeOK);
        emit(firstReadInvoke);
        emit(firstReadOK);
        emit(secondReadInvoke);
        emit(secondReadOK);
    }
}
