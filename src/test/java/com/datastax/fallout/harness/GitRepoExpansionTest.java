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

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import com.datastax.fallout.components.common.spec.GitClone;

import static com.datastax.fallout.assertj.Assertions.assertThat;

public class GitRepoExpansionTest
{
    public static Object[][] cases()
    {
        return new Object[][] {
            {"short", "git@github.com:riptano/short.git"},
            {"short.git", "git@github.com:riptano/short.git"},
            {"riptano/longer", "git@github.com:riptano/longer.git"},
            {"riptano/longer.git", "git@github.com:riptano/longer.git"},
            {"other/longer", "git@github.com:other/longer.git"},
            {"other/longer.git", "git@github.com:other/longer.git"},
            {"git@github.com:riptano/full", "git@github.com:riptano/full.git"},
            {"git@github.com:riptano/full.git", "git@github.com:riptano/full.git"},
            {"git@github.com:other/full", "git@github.com:other/full.git"},
            {"git@github.com:other/full.git", "git@github.com:other/full.git"},
            {"https://some-other-git-repo.com/bing/bong", "https://some-other-git-repo.com/bing/bong.git"},
            {"ssh://git@github.com/riptano/short", "ssh://git@github.com/riptano/short.git"},
        };
    }

    @ParameterizedTest(name = "{0} expands to {1}")
    @MethodSource("cases")
    public void short_repo_params_are_correctly_expanded(String given, String expected)
    {
        assertThat(GitClone.expandShortRepo(given)).isEqualTo(expected);
    }
}
