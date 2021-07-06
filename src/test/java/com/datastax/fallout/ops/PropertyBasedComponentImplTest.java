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

import java.util.Arrays;
import java.util.stream.Stream;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import com.datastax.fallout.harness.ArtifactChecker;
import com.datastax.fallout.harness.Module;
import com.datastax.fallout.util.component_discovery.ComponentFactory;
import com.datastax.fallout.util.component_discovery.ServiceLoaderComponentFactory;

import static com.datastax.fallout.assertj.Assertions.assertThat;

/**
 * Tests for validating metadata on PropertyBasedComponents
 */
public class PropertyBasedComponentImplTest
{
    private static ComponentFactory componentFactory;

    @BeforeAll
    static void loadComponents()
    {
        componentFactory = new ServiceLoaderComponentFactory();
    }

    static Stream<Arguments> componentsAndAllowedPrefixes()
    {
        return Stream
            .of(
                Pair.of(Provisioner.class,
                    new String[] {"fallout.provisioner", "test.provisioner"}),
                Pair.of(ConfigurationManager.class,
                    new String[] {"fallout.configuration.management", "test.configuration.management"}),
                Pair.of(Module.class,
                    new String[] {"fallout.module", "test.module"}),
                Pair.of(ArtifactChecker.class,
                    new String[] {"fallout.artifact_checkers", "test.artifact_checkers"})
            )
            // expand the above into a list of components and allowed prefixes
            .flatMap(classAndPrefixes -> componentFactory.exampleComponents(classAndPrefixes.getLeft()).stream()
                .map(component -> Arguments.of(component, classAndPrefixes.getRight())));

    }

    @ParameterizedTest()
    @MethodSource("componentsAndAllowedPrefixes")
    public void component_only_uses_allowed_prefixes(PropertyBasedComponent component, String[] allowedPrefixes)
    {
        String prefix = component.prefix();
        assertThat(prefix)
            .matches(prefix_ -> Arrays.stream(allowedPrefixes).anyMatch(prefix_::startsWith),
                String.format("must start with one of %s", Arrays.toString(allowedPrefixes)))
            .endsWith(".");
    }
}
