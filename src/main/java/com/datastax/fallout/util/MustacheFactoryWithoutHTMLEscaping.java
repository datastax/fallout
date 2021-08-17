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
package com.datastax.fallout.util;

import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.io.Writer;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import com.github.mustachejava.DefaultMustacheFactory;
import com.github.mustachejava.Mustache;
import com.github.mustachejava.MustacheException;
import com.github.mustachejava.reflect.Guard;
import com.github.mustachejava.reflect.MissingWrapper;
import com.github.mustachejava.reflect.ReflectionObjectHandler;

import com.datastax.fallout.exceptions.InvalidConfigurationException;

public class MustacheFactoryWithoutHTMLEscaping extends DefaultMustacheFactory
{
    /** Disable HTML escaping: it's not wanted in fallout templates */
    @Override
    public void encode(String value, Writer writer)
    {
        try
        {
            writer.write(value);
        }
        catch (IOException e)
        {
            throw new MustacheException("Failed to write value: " + value, e);
        }
    }

    public static String renderWithScopes(String definition, List<Map<String, Object>> scopes)
    {
        final var mustacheFactory = new MustacheFactoryWithoutHTMLEscaping();
        final var missingTags = new HashSet<String>();

        // Inspired by https://github.com/spullara/mustache.java/issues/1#issuecomment-449716760
        mustacheFactory.setObjectHandler(new ReflectionObjectHandler() {
            @Override
            protected MissingWrapper createMissingWrapper(String name, List<Guard> guards)
            {
                missingTags.add(name);
                return super.createMissingWrapper(name, guards);
            }
        });

        final Mustache mustache = mustacheFactory.compile(new StringReader(definition), "test yaml");

        final StringWriter stringWriter = new StringWriter(definition.length() * 2);
        mustache.execute(stringWriter, scopes.toArray());

        if (!missingTags.isEmpty())
        {
            throw new InvalidConfigurationException("Some template tags were not given values: " +
                String.join(", ", missingTags));
        }

        return stringWriter.toString();
    }
}
