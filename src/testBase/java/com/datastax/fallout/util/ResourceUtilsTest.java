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
package com.datastax.fallout.util;

import java.io.IOException;
import java.util.HashMap;

import com.google.common.io.Resources;
import org.junit.jupiter.api.Test;

import com.datastax.fallout.components.chaos_mesh.ChaosMeshConfigurationManager;

import static com.datastax.fallout.assertj.Assertions.assertThat;
import static com.datastax.fallout.util.ResourceUtils.walkResourceTree;

/** This is in testBase because we need to check that it works for both resources-in-a-jar (integrationTest) and
 *  resources-in-a-directory (test) */
public class ResourceUtilsTest
{
    @Test
    public void walkResourceTree_finds_helm_charts() throws IOException
    {
        final var chartYamlContent = Resources.toByteArray(
            Resources.getResource(ChaosMeshConfigurationManager.class, "helm-chart/Chart.yaml"));

        final var resources = new HashMap<String, byte[]>();

        walkResourceTree(ChaosMeshConfigurationManager.class, "helm-chart", resources::put);

        assertThat(resources).hasEntrySatisfying("helm-chart/Chart.yaml",
            content -> assertThat(content).isEqualTo(chartYamlContent));
    }
}
