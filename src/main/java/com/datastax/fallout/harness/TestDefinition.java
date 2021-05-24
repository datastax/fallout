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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.io.StringWriter;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import com.github.mustachejava.Mustache;
import com.github.mustachejava.reflect.Guard;
import com.github.mustachejava.reflect.MissingWrapper;
import com.github.mustachejava.reflect.ReflectionObjectHandler;
import org.apache.commons.lang3.tuple.Pair;

import com.datastax.fallout.exceptions.InvalidConfigurationException;
import com.datastax.fallout.util.MustacheFactoryWithoutHTMLEscaping;
import com.datastax.fallout.util.YamlUtils;

import static com.datastax.fallout.util.YamlUtils.loadYaml;

public class TestDefinition
{
    public static String getDereferencedYaml(String yaml)
    {
        Pair<Optional<String>, String> localDefaultsAndDefinition = splitDefaultsAndDefinition(yaml);
        Optional<String> localDefaults = localDefaultsAndDefinition.getLeft();
        String localDefinition = localDefaultsAndDefinition.getRight();

        if (localDefinition.startsWith("yaml_url: "))
        {
            if (localDefaults.isPresent())
            {
                String exceptionMsg =
                    "Local defaults are not allowed when importing from a remote yaml. Found the following defaults: " +
                        localDefaults.toString();
                throw new InvalidConfigurationException(exceptionMsg);
            }
            return importRemoteYaml(localDefinition);
        }
        return yaml;
    }

    public static Pair<Optional<String>, String> splitDefaultsAndDefinition(String yaml)
    {
        Optional<String> defaults = Optional.empty();
        String[] docs = yaml.split("(\\n|\\A)---\\n?");
        String definition = docs[docs.length - 1];
        if (docs.length > 1)
        {
            defaults = Optional.of(docs[0]);
        }
        return Pair.of(defaults, definition);
    }

    public static String expandTemplate(String yaml, Map<String, Object> templateParams)
    {
        final Pair<Optional<String>, String> defaultsAndDefinition =
            splitDefaultsAndDefinition(getDereferencedYaml(yaml));
        Map<String, Object> defaults = loadDefaults(defaultsAndDefinition.getLeft());
        String definition = defaultsAndDefinition.getRight();
        return renderDefinitionWithScopes(definition, List.of(defaults, templateParams));
    }

    public static String renderDefinitionWithScopes(String definition, List<Map<String, Object>> scopes)
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

    public static Map<String, Object> loadDefaults(Optional<String> defaultsYaml)
    {
        return defaultsYaml.map(YamlUtils::loadYaml).orElse(Map.of());
    }

    private static String importRemoteYaml(String yaml)
    {
        Map<String, Object> yamlMap = loadYaml(yaml);

        URL yaml_url;
        try
        {
            yaml_url = new URL(yamlMap.get("yaml_url").toString());
        }
        catch (MalformedURLException e)
        {
            throw new InvalidConfigurationException("The provided yaml_url is not a valid URL.", e);
        }

        String result;
        try
        {
            result = new BufferedReader(new InputStreamReader(yaml_url.openStream(), StandardCharsets.UTF_8))
                .lines().collect(Collectors.joining("\n"));
        }
        catch (IOException e)
        {
            throw new InvalidConfigurationException("Could not read a valid test yaml from the provided yaml_url.", e);
        }

        result = String.format("# Imported from yaml_url: %s\n", yaml_url.toString()) + result;
        return result;
    }
}
