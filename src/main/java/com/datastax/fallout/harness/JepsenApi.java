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
package com.datastax.fallout.harness;

import clojure.java.api.Clojure;
import clojure.lang.IFn;
import clojure.lang.Keyword;
import jepsen.generator.Generator;

import static com.datastax.fallout.harness.ClojureApi.require;

public class JepsenApi
{
    /** This is a namespace class: prevent instantiation */
    private JepsenApi()
    {
    }

    /** Force loading the class, and thus initialize clojure (which is slow) */
    public static void preload()
    {
    }

    static
    {
        require("jepsen.core");
        require("jepsen.util");
        require("jepsen.tests");
        require("jepsen.generator");
        require("jepsen.checker");
    }

    public static final IFn relativeTimeOriginBinding = Clojure.var("jepsen.util", "*relative-time-origin*");
    static final IFn run = Clojure.var("jepsen.core", "run!");
    static final IFn noop = Clojure.var("jepsen.tests", "noop-test");
    static final IFn phasesGen = Clojure.var("jepsen.generator", "phases");
    static final IFn onceGen = Clojure.var("jepsen.generator", "once");
    static final Generator voidGen =
        (Generator) ClojureApi.deref.invoke(Clojure.var("jepsen.generator", "void"));
    static final IFn conductorGen = Clojure.var("jepsen.generator", "conductor");
    static final IFn concatGen = Clojure.var("jepsen.generator", "concat");
    static final IFn onGen = Clojure.var("jepsen.generator", "on");
    static final IFn checkerCompose = Clojure.var("jepsen.checker", "compose");

    static final Keyword NAME = Keyword.intern("name");
    static final Keyword NODES = Keyword.intern("nodes");
    static final Keyword CONCURRENCY = Keyword.intern("concurrency");
    static final Keyword CONDUCTORS = Keyword.intern("conductors");
    static final Keyword GENERATOR = Keyword.intern("generator");
    static final Keyword CHECKER = Keyword.intern("checker");
    static final Keyword END = Keyword.intern("end");
    static final Keyword INFO = Keyword.intern("info");
    public static final Keyword VALUE = Keyword.intern("value");
    public static final Keyword TYPE = Keyword.intern("type");
    public static final Keyword TIME = Keyword.intern("time");
    public static final Keyword PROCESS = Keyword.intern("process");
    static final Keyword MODULE = Keyword.intern("module");
    static final Keyword MEDIATYPE = Keyword.intern("mediatype");
    static final Keyword ENSEMBLE = Keyword.intern("ensemble");
    static final Keyword ACTIVEHISTORIES = Keyword.intern("active-histories");
    public static final Keyword VALID = Keyword.intern("valid?");
}
