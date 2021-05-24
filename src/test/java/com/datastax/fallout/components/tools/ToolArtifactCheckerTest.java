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
package com.datastax.fallout.components.tools;

import java.util.Map;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import com.datastax.fallout.harness.EnsembleFalloutTest;
import com.datastax.fallout.service.FalloutConfiguration;

import static com.datastax.fallout.assertj.Assertions.assertThat;

class ToolArtifactCheckerTest extends EnsembleFalloutTest<FalloutConfiguration>
{
    public ToolArtifactCheckerTest()
    {
        super(EnsembleFalloutTest.YamlFileSource.USE_TEST_CLASS_RESOURCES);
    }

    @ParameterizedTest
    @CsvSource({
        "Hello World!",
        "a b c 1 2 3"
    })
    public void no_op_tool_runs_correctly(String args)
    {
        maybeAssertYamlFileRunsAndPasses("fake-no-op.yaml", Map.of("no_op_args", args));
        assertThat(toFullArtifactPath("no-op-was-here"))
            .hasContent(args);
    }
}
