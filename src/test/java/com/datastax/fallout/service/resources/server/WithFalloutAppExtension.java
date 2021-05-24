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
package com.datastax.fallout.service.resources.server;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.RegisterExtension;

import com.datastax.fallout.service.resources.FalloutAppExtensionBase;
import com.datastax.fallout.service.resources.FalloutAppExtensionBase.FalloutServiceResetExtension;

@Tag("requires-db")
public class WithFalloutAppExtension<FA extends FalloutAppExtensionBase<?, ?>>
{
    private final FA falloutAppExtension;

    @RegisterExtension
    public final FalloutServiceResetExtension falloutServiceResetExtension;

    protected WithFalloutAppExtension(FA falloutAppExtension)
    {
        this.falloutAppExtension = falloutAppExtension;
        this.falloutServiceResetExtension = falloutAppExtension.resetExtension();
    }

    protected FA getFalloutApp()
    {
        return falloutAppExtension;
    }

    protected FalloutServiceResetExtension getFalloutServiceResetExtension()
    {
        return falloutServiceResetExtension;
    }
}
