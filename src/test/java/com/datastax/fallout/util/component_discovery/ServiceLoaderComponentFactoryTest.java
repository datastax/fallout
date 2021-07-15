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
package com.datastax.fallout.util.component_discovery;

import com.google.auto.service.AutoService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static com.datastax.fallout.assertj.Assertions.assertThat;

public class ServiceLoaderComponentFactoryTest
{
    public static class Womble implements NamedComponent
    {
        private final String name;

        public Womble(String name)
        {
            this.name = name;
        }

        @Override
        public String name()
        {
            return name;
        }
    }

    private ComponentFactory componentFactory;

    @BeforeEach
    public void createComponentFactory()
    {
        componentFactory = new ServiceLoaderComponentFactory();
    }

    @AutoService(Womble.class)
    public static class Orinoco1 extends Womble
    {
        public Orinoco1()
        {
            super("orinoco");
        }
    }

    @AutoService(Womble.class)
    public static class Orinoco2 extends Womble
    {
        public Orinoco2()
        {
            super("orinoco");
        }
    }

    @Test
    public void create_uses_class_name_ordering()
    {
        assertThat(componentFactory.create(Womble.class, "orinoco"))
            .isOfAnyClassIn(Orinoco1.class);
        assertThat(componentFactory.exampleComponents(Womble.class))
            .hasExactlyElementsOfTypes(Orinoco1.class);
    }
}
