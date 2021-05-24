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
package com.datastax.fallout.util;

import java.util.ListIterator;
import java.util.function.Predicate;

public class Iterators
{
    /** Advance the iterator until okay is not true, and then leave the iterator just before that point */
    public static <T> ListIterator<T> takeWhile(ListIterator<T> it, Predicate<T> okay)
    {
        while (it.hasNext())
        {
            if (!okay.test(it.next()))
            {
                it.previous();
                return it;
            }
        }
        return it;
    }
}
