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

/** Encapsulates the creation of named components of a specific type */
public interface NamedComponentFactory<Component extends NamedComponent>
{
    /** Create a new component with the specific name, or return null if none exists */
    Component create(String name);

    /** Get example instances of all the possible components */
    Collection<Component> exampleComponents();
}
