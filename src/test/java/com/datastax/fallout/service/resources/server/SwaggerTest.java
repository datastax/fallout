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

import javax.ws.rs.core.Response;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import com.datastax.fallout.service.FalloutConfiguration.ServerMode;
import com.datastax.fallout.service.resources.FalloutAppExtension;
import com.datastax.fallout.test.utils.WithPersistentTestOutputDir;
import com.datastax.fallout.util.Exceptions;

import static com.datastax.fallout.assertj.Assertions.assertThat;

@Tag("requires-db")
public class SwaggerTest extends WithPersistentTestOutputDir
{
    public static ServerMode[] serverModes()
    {
        return ServerMode.values();
    }

    @ParameterizedTest
    @MethodSource("serverModes")
    public void swagger_json_can_be_retrieved(ServerMode serverMode)
    {
        FalloutAppExtension falloutAppExtension = new FalloutAppExtension(serverMode);
        Exceptions.runUnchecked(() -> falloutAppExtension.before(persistentTestOutputDir()));

        FalloutAppExtension.FalloutServiceResetExtension falloutServiceResetExtension =
            falloutAppExtension.resetExtension();
        falloutServiceResetExtension.before();

        assertThat(falloutServiceResetExtension.anonApi().build("swagger.json").get())
            .hasStatusInfo(Response.Status.OK);

        falloutAppExtension.after();
    }
}
