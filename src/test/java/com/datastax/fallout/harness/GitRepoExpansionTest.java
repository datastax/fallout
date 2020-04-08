/*
 * Copyright 2020 DataStax, Inc.
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

import java.util.Arrays;
import java.util.Collection;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import com.datastax.fallout.harness.specs.GitClone;

import static org.assertj.core.api.Assertions.assertThat;

@RunWith(Parameterized.class)
public class GitRepoExpansionTest
{
    @Parameters(name = "{0} expands to {1}")
    public static Collection<Object[]> cases()
    {
        return Arrays.asList(new Object[][] {
            {"mine/other", "git@github.com:mine/other.git"},
            {"https://git.datastax.com/bing/bong", "https://git.datastax.com/bing/bong.git"},

            {"git@github.com:datastax/dse-metric-reporter-dashboards.git",
                "git@github.com:datastax/dse-metric-reporter-dashboards.git"},
            {"git@github.com:datastax/python-driver.git", "git@github.com:datastax/python-driver.git"},
            {"git@github.com:datastax/java-driver.git", "git@github.com:datastax/java-driver.git"},

            // irregular examples found within Fallout
            {"git://github.com/tjake/mvbench.git", "git://github.com/tjake/mvbench.git"},
            {"https://github.com/datastax/spark-cassandra-stress.git",
                "https://github.com/datastax/spark-cassandra-stress.git"}
        });
    }

    @Parameter(0)
    public String given;

    @Parameter(1)
    public String expected;

    @Test
    public void short_repo_params_are_correctly_expanded()
    {
        assertThat(GitClone.expandShortRepo(given)).isEqualTo(expected);
    }
}
