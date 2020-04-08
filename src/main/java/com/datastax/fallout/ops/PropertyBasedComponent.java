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
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import org.slf4j.Logger;

/**
 * Interface for objects that may be created as part of a definition for a test.
 * This includes ConfigurationManagers, Provisioners, Modules, and Checkers.
 *
 * @see com.datastax.fallout.ops.ConfigurationManager
 * @see com.datastax.fallout.ops.Provisioner
 * @see com.datastax.fallout.harness.Checker
 * @see com.datastax.fallout.harness.Module
 */
public interface PropertyBasedComponent
{
    /**
     * @return the prefix for properties of this Component
     */
    String prefix();

    /**
     * @return a unique name for this Component
     */
    String name();

    /**
     * @return the long description of this Component
     */
    String description();

    /**
     * @return a string linking to example usage of this component. Implementations should use the form of
     * https://github.com/datastax/fallout/tree/master/examples/{exampleUsage} for the URI. Leaving the default
     * implementation will result in a link to https://github.com/datastax/fallout/tree/master/examples/
     */
    default Optional<String> exampleUsage()
    {
        return Optional.empty();
    }

    /**
     * @return the list of properties this Component will accept
     */
    default List<PropertySpec> getPropertySpecs()
    {
        return Collections.emptyList();
    }

    /**
     * Flag to hide this component when in shared mode
     *
     * Useful for tools that would run locally only (like LocalProvisioner/CCM)
     *
     * @see com.datastax.fallout.service.FalloutConfiguration#getIsSharedEndpoint
     */
    default boolean disabledWhenShared()
    {
        return false;
    }

    void setInstanceName(String name);

    String getInstanceName();

    /**
     * Method to perform complex validation of a component's properties, i.e.,
     * if a username is set, a password must be as well.
     *
     * Should throw PropertySpec.ValidationException if an invalid property
     * combination is found.
     */
    default void validateProperties(PropertyGroup properties) throws PropertySpec.ValidationException
    {
    }

    /**
     * Method to perform checking of the prefixes of the component's PropertySpecs
     */
    default boolean validatePrefixes(Logger logger)
    {
        String prefix = prefix();
        List<String> wrongPrefixPropertySpecs = new ArrayList<>();
        // iterate through PropertySpecs of component, verify they all have correct prefix
        for (PropertySpec spec : getPropertySpecs())
        {
            if (!spec.prefix().equals(prefix))
            {
                wrongPrefixPropertySpecs.add(spec.name());
            }
        }
        // log error if we found a PropertySpec with incorrect prefix
        boolean validPrefixes = wrongPrefixPropertySpecs.isEmpty();
        if (!validPrefixes)
        {
            logger.error(String.format(
                "Current component prefix is %s. Found invalid component prefixes for following PropertySpecs: %s",
                prefix, wrongPrefixPropertySpecs));
        }
        return validPrefixes;
    }
}
