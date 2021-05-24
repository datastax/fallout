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

import java.util.Map;

import org.junit.jupiter.api.Test;

import static com.datastax.fallout.assertj.Assertions.assertThat;
import static com.datastax.fallout.harness.TestDefinition.expandTemplate;

public class TestDefinitionTest
{
    @Test
    public void expandTemplate_does_not_use_HTML_escaping()
    {
        String yaml =
            "foo: {{foo}}\n" +
                "bar: {{{foo}}}\n";

        String valueWithHTMLescapableChars = "-Dfoo=true -Dbar=false";
        assertThat(expandTemplate(yaml, Map.of("foo", valueWithHTMLescapableChars)))
            .isEqualTo(String.format(
                "foo: %1$s\n" +
                    "bar: %1$s\n",
                valueWithHTMLescapableChars));
    }
}
