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

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import com.datastax.fallout.exceptions.InvalidConfigurationException;
import com.datastax.fallout.service.FalloutConfiguration;

import static com.datastax.fallout.assertj.Assertions.assertThatExceptionOfType;

public class NestedPhaseTest extends EnsembleFalloutTest<FalloutConfiguration>
{
    @Test
    public void testSerialTopLevelPhases()
    {
        maybeAssertYamlFileRunsAndPasses("nested-phase-yamls/serial-top-phases.yaml");
    }

    @Test
    public void testSerialSubPhases()
    {
        maybeAssertYamlFileRunsAndPasses("nested-phase-yamls/serial-sub-phases.yaml");
    }

    @Test
    public void testNSubPhasesAtDepthN()
    {
        maybeAssertYamlFileRunsAndPasses("nested-phase-yamls/n-depth-phases.yaml");
    }

    @Test
    public void testSubPhasesCompleteBeforeNextTopPhaseBegins()
    {
        maybeAssertYamlFileRunsAndPasses("nested-phase-yamls/multiple-phases-with-subphases.yaml");
    }

    @Test
    public void testSubPhasesAlongsideModules()
    {
        maybeAssertYamlFileRunsAndPasses("nested-phase-yamls/modules-with-subphases.yaml");
    }

    @Test
    public void testDepthTwoSubPhases()
    {
        maybeAssertYamlFileRunsAndPasses("nested-phase-yamls/depth-two-phases.yaml");
    }

    @Test
    public void testBadPhaseOrderingFails()
    {
        maybeAssertYamlFileRunsAndFails("nested-phase-yamls/bad-regex.yaml");
    }

    @Test
    public void testDuplicateModuleNamesInNestedPhases()
    {
        assertThatExceptionOfType(InvalidConfigurationException.class)
            .isThrownBy(() -> createActiveTestRunBuilder()
                .withTestDefinitionFromYaml(readYamlFile("nested-phase-yamls/duplicate-modules.yaml"))
                .build())
            .withMessage("Duplicate module or subphase aliases: text3.1");
    }

    @Disabled("To be fixed in FAL-780.")
    @Test
    public void testNestedPhaseLifetimeModules()
    {
        maybeAssertYamlFileRunsAndPasses("nested-phase-yamls/nested-phase-lifetime-modules.yaml");
    }
}
