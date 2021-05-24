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
package com.datastax.fallout.service.cli;

import java.nio.file.Paths;
import java.util.Optional;

import com.datastax.fallout.service.FalloutConfiguration;
import com.datastax.fallout.service.FalloutServiceBase;

/** A command that starts the "main" server i.e. the server that accepts user requests */
public class FalloutMainServerCommand<FC extends FalloutConfiguration> extends FalloutServerCommand<FC>
{
    protected FalloutMainServerCommand(FalloutServiceBase<FC> falloutService, String name, String description)
    {
        super(falloutService, name, description);
    }

    @Override
    protected String getPidFileLocation()
    {
        return FalloutConfiguration.getPidFile(Paths.get("$FALLOUT_HOME"), Optional.empty()).toString();
    }
}
