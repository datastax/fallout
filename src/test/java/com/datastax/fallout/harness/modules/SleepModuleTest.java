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
package com.datastax.fallout.harness.modules;

import java.util.HashMap;

import com.google.common.net.MediaType;
import org.assertj.core.data.Offset;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import com.datastax.fallout.harness.Operation;
import com.datastax.fallout.ops.Ensemble;
import com.datastax.fallout.ops.PropertyGroup;
import com.datastax.fallout.ops.WritablePropertyGroup;

import static org.assertj.core.api.Assertions.assertThat;

public class SleepModuleTest
{
    @Mock
    private Ensemble ensemble;

    private SleepModule module;

    @Before
    public void setUp()
    {
        module = new SleepModule()
        {
            @Override
            public void emit(Operation.Type type, MediaType mimeType, Object value)
            {
            }
        };
    }

    @Test
    public void testSleepsGivenDuration()
    {
        HashMap<String, String> prefixConfig = new HashMap<String, String>();
        prefixConfig.put(module.prefix() + "duration", "3s");
        PropertyGroup propertyGroup = new WritablePropertyGroup(prefixConfig);
        module.setup(ensemble, propertyGroup);

        Long startTime = System.currentTimeMillis();
        module.run(ensemble, propertyGroup);
        Long duration = System.currentTimeMillis() - startTime;

        assertThat(duration).isCloseTo(3000L, Offset.offset(500L));
    }

}
