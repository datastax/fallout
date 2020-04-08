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

import java.util.Collection;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

public class UtilsTest
{
    @Test
    public void parseJsonSingletonMap_parses_a_list_containing_a_single_directory()
    {
        String singleDirectoryOutput = "{\"0\": [\n\"/var/lib/cassandra/data\"\n]\n}";
        Collection<String> simpleResult = Utils.parseJsonSingletonMap(singleDirectoryOutput);
        assertThat(simpleResult).containsExactly("/var/lib/cassandra/data");
    }

    @Test
    public void parseJsonSingletonMap_parses_a_list_containing_multiple_directories()
    {
        String multipleDirectoriesOutput =
            "{\n\"4\": [\n\"/var/lib/cassandra/data\",\n\"/var/lib/some/other/path\"\n]\n}";
        Collection<String> complexResult = Utils.parseJsonSingletonMap(multipleDirectoriesOutput);
        assertThat(complexResult).containsExactly("/var/lib/cassandra/data", "/var/lib/some/other/path");
    }

    @Test
    public void parseJsonSingletonMap_throws_on_bad_json()
    {
        String badOutput = "some other text which isn't json";
        assertThatExceptionOfType(IllegalArgumentException.class)
            .isThrownBy(() -> Utils.parseJsonSingletonMap(badOutput));
    }

    @Test
    public void parseJsonSingletonMap_throws_on_non_singleton_maps()
    {
        String nonSingletonJsonMap =
            "{\n\"0\": [\n\"/var/lib/cassandra/data\"\n],\n\"1\": [\n\"/mnt/cass_data_disks/data1\"\n]\n}";
        assertThatExceptionOfType(IllegalArgumentException.class)
            .isThrownBy(() -> Utils.parseJsonSingletonMap(nonSingletonJsonMap));
    }

    @Test
    public void parseJsonSingletonMap_parses_a_single_directory()
    {
        String singleDirectoryOutput = "{\"0\": \"/var/lib/cassandra/data\"\n}";
        String simpleResult = Utils.parseJsonSingletonMap(singleDirectoryOutput);
        assertThat(simpleResult).isEqualTo("/var/lib/cassandra/data");
    }
}
