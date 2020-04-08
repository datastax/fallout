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
package com.datastax.fallout.service.views;

import java.util.List;

import com.datastax.fallout.FalloutVersion;
import com.datastax.fallout.runner.QueuingTestRunner;
import com.datastax.fallout.service.resources.server.ComponentResource;

/** Contains things that don't change from one UI page to the next. */
public final class MainView
{
    private final List<ComponentResource.ComponentType> componentMenu;
    private final QueuingTestRunner testRunner;
    public final String assetsRoot;
    public final String falloutVersion = FalloutVersion.getVersion();

    public MainView(List<ComponentResource.ComponentType> componentMenu,
        QueuingTestRunner testRunner, String assetsRoot)
    {
        this.componentMenu = componentMenu;
        this.testRunner = testRunner;
        this.assetsRoot = assetsRoot;
    }

    public List<ComponentResource.ComponentType> getComponentMenu()
    {
        return componentMenu;
    }

    public QueuingTestRunner getTestRunner()
    {
        return testRunner;
    }
}
