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
package com.datastax.fallout.util;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.yaml.snakeyaml.LoaderOptions;
import org.yaml.snakeyaml.Yaml;

import com.datastax.fallout.exceptions.InvalidConfigurationException;

public class YamlUtils
{
    /**
     * Parse a String from the YAML test definition that possibly contains newline characters.
     *
     * If the user entered a String with the YAML folded style, the String will end with a newline char. If instead the
     * YAML literal style was chosen, the newlines will be preserved. This method replaces all the newlines characters
     * by spaces so that the value can be used in a bash command.
     *
     * @param s a String that can contain newlines
     *
     * @return the String with newlines replaced by spaces, or {@code null} if {@code s} is {@code null}
     *
     * @see <a href="http://www.yaml.org/spec/1.2/spec.html#id2796251">Folded style spec</a>
     * @see <a href="http://www.yaml.org/spec/1.2/spec.html#id2795688">Literal style spec</a>
     */
    public static String removeNewlines(String s)
    {
        return s == null ? null : s.replace("\n", " ");
    }

    private static Yaml yaml()
    {
        final LoaderOptions loaderOptions = new LoaderOptions();
        loaderOptions.setAllowDuplicateKeys(false);
        return new Yaml(loaderOptions);
    }

    @SuppressWarnings("unchecked")
    public static Map<String, Object> loadYaml(String yaml)
    {
        try
        {
            return (Map<String, Object>) yaml().load(yaml);
        }
        catch (Exception e)
        {
            throw new InvalidConfigurationException(e);
        }
    }

    public static JsonNode loadYamlDocument(String yaml)
    {
        return loadYamlDocument(yaml, null);
    }

    public static JsonNode loadYamlDocument(String yaml, String nodePath)
    {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        JsonNode node = Exceptions.getUncheckedIO(() -> mapper.readTree(yaml));
        return Optional.ofNullable(nodePath).map(node::at).orElse(node);
    }

    public static List<JsonNode> loadYamlDocuments(String yamlMultiDoc)
    {
        return Arrays.stream(yamlMultiDoc.split("---"))
            .map(YamlUtils::loadYamlDocument)
            .filter(n -> !n.isEmpty())
            .collect(Collectors.toList());
    }

    public static String dumpYaml(Object data)
    {
        return yaml().dump(data);
    }
}
