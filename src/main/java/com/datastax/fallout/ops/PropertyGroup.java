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
import java.util.Map;

public interface PropertyGroup
{
    default void validateFull(List<PropertySpec<?>> propertySpecs) throws PropertySpec.ValidationException
    {
        validate(propertySpecs, false, true);
    }

    default void validate(List<PropertySpec<?>> propertySpecs) throws PropertySpec.ValidationException
    {
        validate(propertySpecs, false, false);
    }

    default void validate(List<PropertySpec<?>> propertySpecs, boolean ignoreFalloutProperties)
        throws PropertySpec.ValidationException
    {
        validate(propertySpecs, ignoreFalloutProperties, false);
    }

    void validate(List<PropertySpec<?>> propertySpecs, boolean ignoreFalloutProperties, boolean failForUnknownProps)
        throws PropertySpec.ValidationException;

    enum ExpandRefsMode
    {
        EXPAND_REFS,
        IGNORE_REFS
    }

    Object get(String name, ExpandRefsMode expandRefsMode);

    default boolean hasProperty(String name)
    {
        return get(name, ExpandRefsMode.IGNORE_REFS) != null;
    }

    /** Return an immutable view of the underlying properties */
    Map<String, Object> asMap();
}
