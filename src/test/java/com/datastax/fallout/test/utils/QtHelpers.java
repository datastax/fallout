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
package com.datastax.fallout.test.utils;

import javax.annotation.Nonnull;

import org.quicktheories.core.Gen;
import org.quicktheories.core.RandomnessSource;

public class QtHelpers
{
    private QtHelpers()
    {
        // utility class
    }

    public static <T> Gen<T> named(String name, Gen<T> gen)
    {
        return new Gen<T>() {
            @Override
            public T generate(@Nonnull RandomnessSource in)
            {
                return gen.generate(in);
            }

            @Override
            public String asString(@Nonnull T t)
            {
                return String.format("%s=%s", name, gen.asString(t));
            }
        };
    }
}
