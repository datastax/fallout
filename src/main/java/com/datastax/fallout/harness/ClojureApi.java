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

import clojure.java.api.Clojure;
import clojure.lang.IFn;

public class ClojureApi
{
    static final IFn swap = Clojure.var("clojure.core", "swap!");
    static final IFn conj = Clojure.var("clojure.core", "conj");
    static final IFn get = Clojure.var("clojure.core", "get");
    static final IFn deref = Clojure.var("clojure.core", "deref");
    static final IFn assoc = Clojure.var("clojure.core", "assoc");
    static final IFn dissoc = Clojure.var("clojure.core", "dissoc");
    static final IFn merge = Clojure.var("clojure.core", "merge");
    static final IFn into = Clojure.var("clojure.core", "into");
    static final IFn apply = Clojure.var("clojure.core", "apply");
    static final IFn withBindings = Clojure.var("clojure.core", "with-bindings*");
    static final IFn loggerFactory = Clojure.var("clojure.tools.logging", "*logger-factory*");

    private static final IFn require = Clojure.var("clojure.core", "require");

    public static void require(String clojureLibrary)
    {
        require.invoke(Clojure.read(clojureLibrary));
    }

    public static IFn var(String clojureLibrary, String name)
    {
        require(clojureLibrary);
        return Clojure.var(clojureLibrary, name);
    }
}
