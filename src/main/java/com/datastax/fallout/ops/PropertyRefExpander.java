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

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.datastax.fallout.exceptions.InvalidConfigurationException;

/** Responsible for taking a string property values and expanding any <code><<...>></code> references in it */
public class PropertyRefExpander
{
    private static final Pattern PROPERTY_REF_PATTERN = Pattern.compile("<<([^:>]+):([^>]+)>>");
    private final Map<String, Handler> handlers = new HashMap<>();

    public static abstract class Handler
    {
        private final String type;

        protected Handler(String type)
        {
            this.type = type;
        }

        private String getType()
        {
            return type;
        }

        public abstract String expandKey(String refKey);
    }

    public void addHandler(Handler handler)
    {
        if (handlers.containsKey(handler.getType()))
        {
            throw new IllegalArgumentException(
                String.format("PropertyRefExpander already contains handler for type '%s'",
                    handler.getType()));
        }
        handlers.put(handler.getType(), handler);
    }

    public String expandRefs(String propertyValue, Set<String> ignoredRefTypes)
    {
        Matcher matchedPropertyRefs = PROPERTY_REF_PATTERN.matcher(propertyValue);
        return matchedPropertyRefs.replaceAll(matchedPropertyRef -> {
            String propertyRef = matchedPropertyRefs.group();
            String type = matchedPropertyRefs.group(1);
            String key = matchedPropertyRefs.group(2);

            return ignoredRefTypes.contains(type) ?
                propertyRef :
                Optional.ofNullable(handlers.get(type))
                    .map(handler -> handler.expandKey(key))
                    .orElseThrow(() -> new InvalidConfigurationException(
                        String.format("No property reference of type '%s'", type)));
        });
    }
}
