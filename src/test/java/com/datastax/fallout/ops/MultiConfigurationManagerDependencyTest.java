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
package com.datastax.fallout.ops;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;
import org.mockito.quality.Strictness;

import com.datastax.fallout.exceptions.InvalidConfigurationException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.BDDMockito.given;

public class MultiConfigurationManagerDependencyTest
{
    static class ProviderA extends Provider
    {

        protected ProviderA(Node node)
        {
            super(node);
        }

        @Override
        public String name()
        {
            return "ProviderA";
        }
    }

    static class ProviderA1 extends ProviderA
    {
        protected ProviderA1(Node node)
        {
            super(node);
        }
    }

    static class ProviderB extends Provider
    {

        protected ProviderB(Node node)
        {
            super(node);
        }

        @Override
        public String name()
        {
            return "ProviderA";
        }
    }

    static class ProviderB1 extends ProviderB
    {
        protected ProviderB1(Node node)
        {
            super(node);
        }
    }

    @Rule
    public MockitoRule mockitoRule = MockitoJUnit.rule().strictness(Strictness.STRICT_STUBS);

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Mock
    ConfigurationManager cmA;

    @Mock
    ConfigurationManager cmB;

    @Mock
    ConfigurationManager cmA1;

    @Mock
    ConfigurationManager cmB1;

    private void givenProvides(ConfigurationManager cm, Class<? extends Provider> provider)
    {
        given(cm.getAvailableProviders(any())).willReturn(ImmutableSet.of(provider));
    }

    private void givenRequires(ConfigurationManager cm, Class<? extends Provider> provider)
    {
        given(cm.getRequiredProviders(any())).willReturn(ImmutableSet.of(provider));
    }

    private void thenExpectedOrderIs(ConfigurationManager first, ConfigurationManager second)
    {
        MultiConfigurationManager multiCM = new MultiConfigurationManager(ImmutableList.of(second, first),
            new WritablePropertyGroup());
        assertThat(multiCM.getDelegates()).containsExactly(first, second);
    }

    @Test
    public void delegates_are_satisfied_and_produce_the_correct_configure_order()
    {
        givenProvides(cmA, ProviderA.class);
        givenRequires(cmB, ProviderA.class);
        thenExpectedOrderIs(cmA, cmB);
    }

    @Test
    public void delegates_are_satisfied_and_ordered_when_subclasses_are_present()
    {
        givenProvides(cmA1, ProviderA1.class);
        givenRequires(cmB, ProviderA.class);
        thenExpectedOrderIs(cmA1, cmB);
    }

    @Test
    public void exception_is_thrown_when_dependencies_cannot_be_met()
    {
        givenProvides(cmA, ProviderA.class);
        givenRequires(cmB1, ProviderB1.class);
        thrown.expect(InvalidConfigurationException.class);
        thrown.expectMessage("It is impossible to properly configure this set of Configuration Managers!");
        new MultiConfigurationManager(ImmutableList.of(cmA, cmB1), new WritablePropertyGroup());
    }
}
