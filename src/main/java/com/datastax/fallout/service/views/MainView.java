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
package com.datastax.fallout.service.views;

import java.util.List;
import java.util.function.Function;

import com.datastax.fallout.FalloutVersion;
import com.datastax.fallout.runner.QueuingTestRunner;
import com.datastax.fallout.service.resources.server.ComponentResource;

/** Contains things that don't change from one UI page to the next. */
public final class MainView
{
    private final List<ComponentResource.ComponentType> componentTypes;
    private final QueuingTestRunner testRunner;
    public final String assetsRoot;
    private final Function<String, String> hideDisplayedEmailDomains;
    public final String falloutVersion = FalloutVersion.getVersion();

    public MainView(List<ComponentResource.ComponentType> componentTypes,
        QueuingTestRunner testRunner, String assetsRoot,
        Function<String, String> hideDisplayedEmailDomains)
    {
        this.componentTypes = componentTypes;
        this.testRunner = testRunner;
        this.assetsRoot = assetsRoot;
        this.hideDisplayedEmailDomains = hideDisplayedEmailDomains;
    }

    public List<ComponentResource.ComponentType> getComponentTypes()
    {
        return componentTypes;
    }

    public QueuingTestRunner getTestRunner()
    {
        return testRunner;
    }

    public String hideDisplayedEmailDomains(String title)
    {
        return hideDisplayedEmailDomains.apply(title);
    }
}
