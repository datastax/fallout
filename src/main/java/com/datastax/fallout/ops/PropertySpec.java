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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;

/**
 * A Core Internal API for Fallout Ops.
 *
 * Represents user defined parameters passed to Provisioners, Nodes, ConfigurationManagers etc.
 *
 * @see Node
 * @see Provisioner
 * @see ConfigurationManager
 *
 * A PropertySpec defines how to describe, validate, parse user inputs. They are used by Provisioners
 * (for example) as a way for ensure a set of contraints like:
 *   the number of nodes is specified by the user
 *   this number is > 0 and < 100
 *   this number is passed as a parsable Integer.
 *
 * A user can see the list of PropertiesSpecs asked for by a Provisioner, etc and set these values.
 *
 * This input is entered into a PropertyGroup and specified as a K/V pair.
 * Key must be a String (the property name).
 * Value can be a String, List<String>, or Map<String,String>
 *
 *
 * The simplest way to create a PropertySpec is to use the Builder
 * @see PropertySpecBuilder
 */
public interface PropertySpec<T>
{
    /**
     * Example: fallout.provisioner.ccm.cluster.name
     * @return the name of the property
     */
    String name();

    /**
     * Example: cluster.name
     *
     * @see PropertyBasedComponent#prefix()
     *
     * @return the name of the property minus the prefix.
     */
    String shortName();

    /**
     * Example: fallout.provisioner.ccm
     * @return the prefix of the property
     */
    String prefix();

    /**
     * alias to this property
     * (Useful for things like wiring to a UI
     * with common component names)
     *
     * @return the alias name of the property
     */
    Optional<String> alias();

    /**
     * Example "The name of the ccm cluster"
     *
     * @return a text description for this property values
     */
    String describe();

    /**
     * Descriptive category this property is part of
     * @return
     */
    Optional<String> category();

    /**
     * Possible choices for property
     *
     * This is useful for properties with a fixed set of possible values
     *
     * @return the list of possible values OR empty
     */
    Optional<Collection<Value<T>>> options();

    /**
     * Possible parent spec
     *
     * The feature here is two fold.
     * 1. We can organise docs/UI based on this parent
     * so it's easier to understand.
     *
     * 2. We can ignore the required flag of children
     * when the parent spec is not selected.
     *
     * Here's how it works:
     *   The parent spec must be one with only options().
     *   Each option Value must have a category that matches
     *   the category in the child.
     *
     * The PropertySpecBuilder will enforce this.
     *
     * @see #options()
     * @see #category()
     *
     *
     * @return parent property spec if defined
     */
    Optional<PropertySpec> dependsOn();

    /**
     * Get the default value (if set)
     *
     * @return
     */
    Optional<Value<T>> defaultValue();

    /** Get the default value (if set) as YAML */
    Optional<String> defaultValueYaml();

    /**
     * @param propertyGroup
     * @return the Serialized value from the property found in the properties group
     */
    T value(PropertyGroup propertyGroup);

    /**
     * @return the Serialized value from the property found in the properties group of the given object
     */
    default T value(HasProperties propertyObject)
    {
        return value(propertyObject.getProperties());
    }

    /**
     * @param propertyGroup
     * @return the Serialized value from the property found in the properties group or {@link Optional#empty()} if missing
     */
    Optional<T> optionalValue(PropertyGroup propertyGroup);

    default Optional<T> optionalValue(HasProperties propertyObject)
    {
        return optionalValue(propertyObject.getProperties());
    }

    /**
     * @param propertyGroup
     * @return the value wrapper from the property found in the properties group
     */
    Value<T> valueType(PropertyGroup propertyGroup);

    /**
     * @return true if this property is required, false if optional
     */
    boolean isRequired();

    /**
     * Considering dependent properties
     *
     * @see #dependsOn()
     *
     * @return true if this property is required, false if optional
     */
    boolean isRequired(PropertyGroup properties);

    /**
     * To inspect if this property takes arbitrary inputs or must use an option
     *
     * see options()
     *
     * @return true if this property has no value parser, only options
     */
    boolean isOptionsOnly();

    /**
     * If this property uses a regex to validate
     *
     * @return regex pattern
     */
    Optional<String> validationPattern();

    /**
     *
     * Validates the property by looking it up in the propertyGroup
     *
     * We don't pass the value directly from the property group since
     * so we can avoid the caller needing to call PropertyGroup#get
     * @param propertyGroup
     * @return the property name that was used for validation (could be name/alias/deprecated name)
     * @throws com.datastax.fallout.ops.PropertySpec.ValidationException if validation fails
     */
    Optional<String> validate(PropertyGroup propertyGroup) throws ValidationException;

    Optional<String> deprecatedName();

    Optional<String> deprecatedShortName();

    class ValidationException extends IllegalArgumentException
    {
        private static String formatError(List<PropertySpec<?>> failedSpec, String error)
        {
            String specNames = StringUtils.join(failedSpec.stream()
                .map(PropertySpec::name)
                .collect(Collectors.toList()), ", ");
            return "ValidationException for Properties '" + specNames + "': " + error;
        }

        public final List<PropertySpec<?>> failedSpecs;

        public ValidationException(String error)
        {
            super(error);
            this.failedSpecs = new ArrayList<>();
        }

        public ValidationException(PropertySpec<?> failedSpec, String error)
        {
            this(Arrays.asList(failedSpec), error);
        }

        public ValidationException(List<PropertySpec<?>> failedSpecs, String error)
        {
            super(formatError(failedSpecs, error));
            this.failedSpecs = failedSpecs;
        }
    }

    class Value<T>
    {
        public static final Value empty = new Value(null, null, null);

        public final String id;
        public final T value;
        public boolean isDefault = false;
        public final Optional<String> category;

        private Value(String id, T value, String category)
        {
            this.id = id;
            this.value = value;
            this.category = Optional.ofNullable(category);
        }

        public static <T> Value<T> of(String id, T value, String category)
        {
            return new Value<>(id, value, category);
        }

        public static <T> Value<T> of(T value)
        {
            return new Value<>(String.valueOf(value), value, String.valueOf(value));
        }

        public static <T> Value<T> of(T value, String category)
        {
            return new Value<>(String.valueOf(value), value, category);
        }
    }

}
