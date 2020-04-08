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
package com.datastax.fallout.harness;

import java.io.File;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.junit.Assert.assertNotNull;

@RunWith(Parameterized.class)
public class ExampleYamlTest extends EnsembleFalloutTest
{
    private static final String RESOURCE_DIR_SUFFIX = "resources";

    @Parameterized.Parameters(name = "ExampleYaml-{1}")
    public static Collection<Object[]> getExampleFiles() throws Exception
    {
        File examplesDir = new File(ExampleYamlTest.class.getClassLoader().getResource("examples").toURI());
        List<Object[]> res =
            listExamples(examplesDir).map(filename -> new Object[] {examplesDir.getName() + "/" + filename, filename})
                .collect(Collectors.toList());
        // sort by full path
        res.sort(Comparator.comparing(o -> ((String) o[0])));
        return res;
    }

    private static Stream<String> listExamples(File examplesDir)
    {
        return Arrays.stream(examplesDir.listFiles())
            .flatMap(file -> file.isDirectory() && !file.getName().endsWith(RESOURCE_DIR_SUFFIX) ?
                listExamples(file).map(p -> file.getName() + "/" + p) :
                Stream.of(file.getName()))
            .filter(path -> path.endsWith(".yaml"));
    }

    private String exampleFile;

    public ExampleYamlTest(String fullPath, String pathBelowExamplesDir)
    {
        this.exampleFile = "/" + fullPath;
    }

    @Test
    public void testExampleValidation()
    {
        String yaml = readYamlFile(exampleFile);
        try
        {
            ActiveTestRun x = createActiveTestRun(yaml);
            assertNotNull(x);
        }
        catch (Exception e)
        {
            throw new AssertionError("Example Validation failed: " + exampleFile, e);
        }
    }
}
