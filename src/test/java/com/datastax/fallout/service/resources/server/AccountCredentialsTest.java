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

import javax.ws.rs.client.Entity;
import javax.ws.rs.core.Form;
import javax.ws.rs.core.MultivaluedHashMap;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import com.datastax.fallout.service.resources.FalloutAppExtension;
import com.datastax.fallout.service.resources.RestApiBuilder;

import static com.datastax.fallout.assertj.Assertions.assertThat;

public class AccountCredentialsTest extends WithFalloutAppExtension<FalloutAppExtension>
{
    @RegisterExtension
    public static final FalloutAppExtension FALLOUT_SERVICE = new FalloutAppExtension();

    protected AccountCredentialsTest()
    {
        super(FALLOUT_SERVICE);
    }

    private RestApiBuilder userApi()
    {
        return getFalloutServiceResetExtension().userApi();
    }

    private Entity<Form> nebulaAppCredForm(String project, String id, String secret)
    {
        MultivaluedMap<String, String> formData = new MultivaluedHashMap<>();
        formData.add("projectName", project);
        formData.add("appCredsId", id);
        formData.add("appCredsSecret", secret);
        return Entity.form(formData);
    }

    @Test
    public void nebula_app_creds_must_have_unique_project_ids()
    {
        String duplicateId = "an_id";
        var addAppCredRoute = userApi().build(AccountResource.class, "addNebulaAppCred");
        assertThat(addAppCredRoute.post(nebulaAppCredForm("a_project", duplicateId, "a_secret")))
            .hasStatusInfo(Response.Status.OK);
        assertThat(addAppCredRoute.post(nebulaAppCredForm("b_project", duplicateId, "b_secret")))
            .hasStatusInfo(Response.Status.BAD_REQUEST)
            .hasEntity()
            .extracting(res -> res.readEntity(String.class))
            .isEqualTo(String.format("A Nebula application credential with ID (%s) already exists", duplicateId));
    }
}
