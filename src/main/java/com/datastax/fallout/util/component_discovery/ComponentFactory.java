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

import java.util.Collection;

/** Encapsulates construction of typed, named components */
public interface ComponentFactory
{
    /** Create a new instance of the specified class that has the specified name, or return null */
    <Component extends NamedComponent> Component create(Class<Component> clazz, String name);

    /** Return all available components of the specified class */
    <Component extends NamedComponent> Collection<Component> exampleComponents(Class<Component> clazz);
}
