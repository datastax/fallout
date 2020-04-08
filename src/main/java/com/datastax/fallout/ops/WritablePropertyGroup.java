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

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Represents a user defined set of Properties used by Fallout components
 * to configure themselves
 */
public class WritablePropertyGroup implements PropertyGroup
{
    private String prefix = "";
    private static final Logger logger = LoggerFactory.getLogger(WritablePropertyGroup.class);
    private final Map<String, Object> properties;

    @Override
    public void validateFull(List<PropertySpec> propertySpecs) throws PropertySpec.ValidationException
    {
        validate(propertySpecs, false, true);
    }

    @Override
    public void validate(List<PropertySpec> propertySpecs) throws PropertySpec.ValidationException
    {
        validate(propertySpecs, false, false);
    }

    @Override
    public void validate(List<PropertySpec> propertySpecs, boolean ignoreFalloutProperties)
        throws PropertySpec.ValidationException
    {
        validate(propertySpecs, ignoreFalloutProperties, false);
    }

    @Override
    public void validate(List<PropertySpec> propertySpecs, boolean ignoreFalloutProperties, boolean failForUnknownProps)
        throws PropertySpec.ValidationException
    {
        Map<String, List<PropertySpec<?>>> specsByPropName = new HashMap<>();
        for (PropertySpec<?> spec : propertySpecs)
        {
            Set<String> specNames = new HashSet<>();
            specNames.add(spec.name());
            if (spec.alias().isPresent())
            {
                specNames.add(spec.alias().get());
            }
            if (spec.deprecatedName().isPresent())
            {
                specNames.add(spec.deprecatedName().get());
            }
            for (String propName : specNames)
            {
                List<PropertySpec<?>> targetList = specsByPropName.computeIfAbsent(propName, k -> new LinkedList<>());
                targetList.add(spec);
            }
        }
        for (Map.Entry<String, List<PropertySpec<?>>> e : specsByPropName.entrySet())
        {
            String propName = e.getKey();
            List<PropertySpec<?>> specs = e.getValue();
            if (specs.size() > 1)
            {
                String allSpecs = specs.stream()
                    .map(n -> n.toString())
                    .collect(Collectors.joining(", "));
                String error =
                    "Duplicate property '" + propName + "' (name/alias/deprecatedName) in these specs: " + allSpecs;
                throw new PropertySpec.ValidationException(specs.get(0), error);
            }
        }

        Set<String> validatedPropNames = new HashSet<>();
        for (PropertySpec<?> spec : propertySpecs)
        {
            if (ignoreFalloutProperties && spec.alias().isPresent())
            {
                String alias = spec.alias().get();
                if (alias.startsWith(FalloutPropertySpecs.prefix))
                    continue;
            }
            Optional<String> validatedPropName = spec.validate(this);
            if (validatedPropName.isPresent())
            {
                validatedPropNames.add(validatedPropName.get());
            }
        }
        if (failForUnknownProps)
        {
            Set<String> propNamesToIgnore = new HashSet<>();
            propNamesToIgnore.add(FalloutPropertySpecs.generatedClusterNamePropertySpec.name());
            propNamesToIgnore.add(FalloutPropertySpecs.launchRunLevelPropertySpec.name());
            propNamesToIgnore.add(FalloutPropertySpecs.testRunUrl.name());
            propNamesToIgnore.add(FalloutPropertySpecs.testRunId.name());

            Set<String> unknownPropNames = new HashSet<>();
            unknownPropNames.addAll(this.properties.keySet());
            unknownPropNames.removeAll(validatedPropNames);
            unknownPropNames.removeAll(propNamesToIgnore);
            if (!unknownPropNames.isEmpty())
            {
                throw new PropertySpec.ValidationException("Unknown properties detected: " + unknownPropNames);
            }
        }
    }

    public WritablePropertyGroup()
    {
        properties = new HashMap<>();
    }

    public WritablePropertyGroup(Map<String, String> map)
    {
        properties = new HashMap<>(map.size());

        for (Map.Entry<String, String> entry : map.entrySet())
        {
            String value = entry.getValue();

            if (value == null || value.length() == 0)
                continue;

            switch (value.charAt(0))
            {
                case '[':
                    properties.put(entry.getKey(), Utils.fromJsonList(value));
                    break;
                case '{':
                    properties.put(entry.getKey(), Utils.fromJsonMap(value));
                    break;
                case '"':
                    properties.put(entry.getKey(), value.substring(1, value.length() - 1));
                    break;
                default:
                    properties.put(entry.getKey(), value);
            }
        }
    }

    public static WritablePropertyGroup writableCopy(PropertyGroup propertyGroup)
    {
        return new WritablePropertyGroup(propertyGroup.asMap());
    }

    @Override
    public Map<String, String> asMap()
    {
        Map<String, String> map = new HashMap<>(properties.size());

        for (Map.Entry<String, Object> entry : properties.entrySet())
        {
            //Maps,Lists, Strings all encoded as json
            Object v = entry.getValue();
            map.put(entry.getKey(), (v instanceof String ? (String) v : Utils.json(v)));
        }

        return map;
    }

    /**
     * Shortcut method to avoid typing in long property names.
     * Prepends the component's prefix to properties in later calls.
     */
    @VisibleForTesting
    public WritablePropertyGroup with(EnsembleComponent component)
    {
        return with(component.prefix());
    }

    @VisibleForTesting
    public WritablePropertyGroup with(String prefix)
    {
        this.prefix = prefix;
        return this;
    }

    private WritablePropertyGroup putInternal(String name, Object value)
    {
        if (properties.put(prefix + name, value) != null)
            logger.warn("replacing property already defined: " + name);

        return this;
    }

    public WritablePropertyGroup put(String name, Object value)
    {
        if (value == null || value instanceof String || value instanceof List || value instanceof Map ||
            value instanceof Number || value instanceof Boolean)
            return putInternal(name, value);

        logger.warn("PropertyGroup.put encountered unknown type: " + value.getClass() + " value: " + value);
        return putInternal(name, String.valueOf(value));
    }

    public WritablePropertyGroup put(String name, String value)
    {
        return putInternal(name, value);
    }

    public WritablePropertyGroup put(String name, List<String> values)
    {
        return putInternal(name, values);
    }

    public WritablePropertyGroup put(String name, Map<String, String> values)
    {
        return putInternal(name, values);
    }

    public WritablePropertyGroup put(PropertyGroup propertyGroup)
    {
        properties.putAll(propertyGroup.getProperties());
        return this;
    }

    @Override
    public Object get(String name)
    {
        return properties.get(name);
    }

    @Override
    public boolean hasProperty(String name)
    {
        return properties.containsKey(name);
    }

    @Override
    public boolean isEmpty()
    {
        return properties.isEmpty();
    }

    @Override
    public Map<String, Object> getProperties()
    {
        return Collections.unmodifiableMap(properties);
    }

    @Override
    public String toString()
    {
        return "PropertyGroup{" +
            "prefix='" + prefix + '\'' +
            ", properties=" + properties +
            '}';
    }
}
