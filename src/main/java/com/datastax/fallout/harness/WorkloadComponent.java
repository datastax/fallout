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

import com.datastax.fallout.ops.HasProperties;
import com.datastax.fallout.ops.PropertyBasedComponent;
import com.datastax.fallout.ops.PropertyGroup;

public interface WorkloadComponent extends PropertyBasedComponent, HasProperties
{
    void setProperties(PropertyGroup properties);

    /**
     * Validates the ensemble has the proper configuration for the component to work.
     * This includes things such as providers on a specific NodeGroup, or a managed file etc.
     */
    default void validateEnsemble(EnsembleValidator validator)
    {
    }
}
