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
package com.datastax.fallout.ops;

import java.util.List;
import java.util.stream.Stream;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import com.datastax.fallout.harness.ArtifactChecker;
import com.datastax.fallout.harness.Module;

import static com.datastax.fallout.assertj.Assertions.assertThat;

/**
 * Tests for validating metadata on PropertyBasedComponents
 */
public class PropertyBasedComponentImplTest
{
    private static Stream<PropertyBasedComponent> fetchComponents(Class<? extends PropertyBasedComponent> clazz)
    {
        return Utils.loadComponents(clazz).stream().map(component -> (PropertyBasedComponent) component);
    }

    static Stream<PropertyBasedComponent> fetchProvisioners()
    {
        return fetchComponents(Provisioner.class);
    }

    static Stream<PropertyBasedComponent> fetchConfigurationManagers()
    {
        return fetchComponents(ConfigurationManager.class);
    }

    static Stream<PropertyBasedComponent> fetchModules()
    {
        return fetchComponents(Module.class);
    }

    static Stream<PropertyBasedComponent> fetchArtifactCheckers()
    {
        return fetchComponents(ArtifactChecker.class);
    }

    private void validatePrefix(String prefix, String... options)
    {
        try
        {
            assertThat(Stream.of(options)).anyMatch(prefix::startsWith);
            assertThat(prefix).endsWith(".");
        }
        catch (AssertionError e)
        {
            throw new AssertionError(String
                .format("prefix \"%s\" must start with one of the following: %s and end with \".\"", prefix,
                    List.of(options)),
                e);
        }
    }

    @ParameterizedTest(name = "provisioner-prefix-validation-{0}")
    @MethodSource("fetchProvisioners")
    public void testProvisionerPrefixFormatting(PropertyBasedComponent component)
    {
        validatePrefix(component.prefix(), "fallout.provisioner", "test.provisioner");
    }

    @ParameterizedTest(name = "configuration-manager-prefix-validation-{0}")
    @MethodSource("fetchConfigurationManagers")
    public void testConfigurationManagerPrefixFormatting(PropertyBasedComponent component)
    {
        validatePrefix(component.prefix(), "fallout.configuration.management", "test.configuration.management");
    }

    @ParameterizedTest(name = "module-prefix-validation-{0}")
    @MethodSource("fetchModules")
    public void testModulePrefixFormatting(PropertyBasedComponent component)
    {
        validatePrefix(component.prefix(), "fallout.module", "test.module");
    }

    @ParameterizedTest(name = "artifact-checker-prefix-validation-{0}")
    @MethodSource("fetchArtifactCheckers")
    public void testArtifactCheckerPrefixFormatting(PropertyBasedComponent component)
    {
        validatePrefix(component.prefix(), "fallout.artifact_checkers", "test.artifact_checkers");
    }
}
