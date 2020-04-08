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
package com.datastax.fallout.service.cli;

import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Optional;

import com.github.mustachejava.DefaultMustacheFactory;
import com.github.mustachejava.Mustache;
import com.google.common.collect.ImmutableMap;
import io.dropwizard.metrics.MetricsFactory;
import io.dropwizard.metrics.graphite.GraphiteReporterFactory;
import io.dropwizard.setup.Bootstrap;
import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.Subparser;

import com.datastax.fallout.service.FalloutConfiguration;

public class GenerateJmxtransConf extends FalloutCommand
{
    public GenerateJmxtransConf()
    {
        super("generate-jmxtrans-conf",
            "Generate jmxtrans-config.xml for monitoring cassandra; fails with non-zero exit code if " +
                "graphite reporter not specified in configuration YAML");
    }

    @Override
    public void configure(Subparser subparser)
    {
        super.configure(subparser);

        subparser.addArgument("--output")
            .help("specify an output file; if omitted or '-', conf is generated to stdout")
            .setDefault("-");
    }

    private static boolean generateJmxtransConf(
        Writer output, FalloutConfiguration configuration)
    {
        final MetricsFactory metricsFactory = configuration.getMetricsFactory();

        final Optional<GraphiteReporterFactory> graphiteReporterFactory = metricsFactory.getReporters().stream()
            .filter(reporterFactory -> reporterFactory instanceof GraphiteReporterFactory)
            .map(reporterFactory -> (GraphiteReporterFactory) reporterFactory)
            .findFirst();

        return graphiteReporterFactory.map(
            graphiteReporterFactory_ -> {
                final Mustache template = new DefaultMustacheFactory().compile("jmxtrans/jmxtrans-config.xml.mustache");
                template.execute(output, ImmutableMap.of(
                    "collectionIntervalInSeconds", metricsFactory.getFrequency().toSeconds(),
                    "host", graphiteReporterFactory_.getHost(),
                    "port", graphiteReporterFactory_.getPort()));
                return true;
            })
            .orElseGet(() -> {
                System.err.println("No graphite reporter specified in configuration YAML: no config can be generated");
                return false;
            });
    }

    @Override
    protected void run(Bootstrap<FalloutConfiguration> bootstrap,
        Namespace namespace, FalloutConfiguration configuration) throws Exception
    {
        String output = namespace.getString("output");

        try (Writer writer = output.equals("-") ?
            new OutputStreamWriter(System.out, StandardCharsets.UTF_8) :
            Files.newBufferedWriter(Paths.get(output), StandardCharsets.UTF_8))
        {
            if (!generateJmxtransConf(writer, configuration))
            {
                System.exit(1);
            }
        }
    }
}
