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

import java.util.function.Supplier;

public enum CheckResourcesResult
{
    // Note that best-to-worst ordering is depended upon by the current worstCase() implementation
    AVAILABLE,
    UNAVAILABLE,
    FAILED;

    /** Returns the worst case result of this and other */
    public CheckResourcesResult worstCase(CheckResourcesResult other)
    {
        return CheckResourcesResult.values()[Math.max(this.ordinal(), other.ordinal())];
    }

    /** Executes thenDo if this was successful */
    public CheckResourcesResult ifSuccessful(Supplier<CheckResourcesResult> thenDo)
    {
        return wasSuccessful() ?
            thenDo.get() :
            this;
    }

    /** Return if this is AVAILABLE */
    public boolean wasSuccessful()
    {
        return this == AVAILABLE;
    }

    public static CheckResourcesResult fromWasSuccessful(boolean wasSuccessful)
    {
        return wasSuccessful ? AVAILABLE : FAILED;
    }
}
