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
package com.datastax.fallout;

import java.time.Duration;
import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Test;

import com.datastax.fallout.ProcessHelpers.ProcessResult;
import com.datastax.fallout.test.utils.WithPersistentTestOutputDir;

import static com.datastax.fallout.assertj.Assertions.assertThat;

/** Tests that the docker image works */
public class FalloutDockerTest extends WithPersistentTestOutputDir
{
    public static ProcessResult dockerRun(List<String> dockerOpts, List<String> command)
    {
        final var dockerLatestTag = System.getProperty("dockerLatestTag");
        assertThat(dockerLatestTag).isNotNull();

        final var args = ImmutableList.<String>builder()
            .add("docker", "run", "--rm")
            .addAll(dockerOpts)
            .add(dockerLatestTag)
            .addAll(command)
            .build();

        return ProcessHelpers.run(args, Map.of(), Duration.ofMinutes(1));
    }

    public ProcessResult dockerRun(String... command)
    {
        return dockerRun(List.of(), List.of(command));
    }

    @Test
    public void the_version_is_as_expected()
    {
        final var falloutVersion = System.getProperty("falloutVersion");
        assertThat(falloutVersion)
            .startsWith("fallout-");

        assertThat(dockerRun("fallout", "--version"))
            .satisfies(processResult -> {
                assertThat(processResult.exitCode()).isZero();
                assertThat(processResult.stdout()).contains(falloutVersion);
            });
    }

    @Test
    public void bogus_command_fails()
    {
        assertThat(dockerRun("fallout", "--bogus").exitCode())
            .isNotZero();
    }
}
