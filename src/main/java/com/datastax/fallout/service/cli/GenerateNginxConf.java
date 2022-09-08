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
package com.datastax.fallout.service.cli;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import com.github.mustachejava.DefaultMustacheFactory;
import com.github.mustachejava.Mustache;
import io.dropwizard.setup.Bootstrap;
import net.sourceforge.argparse4j.impl.Arguments;
import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.Subparser;

import com.datastax.fallout.service.FalloutConfiguration;
import com.datastax.fallout.service.FalloutServiceBase;
import com.datastax.fallout.util.Exceptions;

public class GenerateNginxConf<FC extends FalloutConfiguration> extends FalloutCommand<FC>
{
    public static class NginxConfParams
    {
        public static final String NGINX_DIRECT_ARTIFACTS_LOCATION = "/artifacts_direct";
        public static final String NGINX_GZIP_ARTIFACTS_LOCATION = "/artifacts_gzip";
        public static final String NGINX_ARTIFACT_ARCHIVE_LOCATION = "/artifacts_archive";
        public static final String NGINX_ARTIFACT_ARCHIVE_DEFAULT_RESOLVER = "1.1.1.1";
        public static final int NGINX_GZIP_MIN_LENGTH = 512 * 1024;
        public final boolean standalone;
        public final int nginxListenPort;
        public final Path nginxRoot;
        public final String archiveResolver;

        public NginxConfParams(boolean standalone, int nginxListenPort, Path nginxRoot, String archiveResolver)
        {
            this.standalone = standalone;
            this.nginxListenPort = nginxListenPort;
            this.nginxRoot = nginxRoot.toAbsolutePath();
            this.archiveResolver = archiveResolver;
        }

        public NginxConfParams(Namespace namespace)
        {
            this(
                namespace.getBoolean("standalone"),
                namespace.getInt("nginxListenPort"),
                Paths.get(namespace.getString("nginxRoot")),
                namespace.getString("archiveResolver"));
        }
    }

    public GenerateNginxConf(FalloutServiceBase<FC> application)
    {
        super(application, "generate-nginx-conf", "Generate fallout.nginx.conf");
    }

    @Override
    public void configure(Subparser subparser)
    {
        super.configure(subparser);

        subparser.addArgument("--standalone")
            .help("generate a standalone nginx configuration for testing")
            .action(Arguments.storeTrue());
        subparser.addArgument("--nginx-listen-port")
            .help("the port the nginx server should listen on")
            .dest("nginxListenPort")
            .type(Integer.class)
            .setDefault(80)
            .action(Arguments.store());
        subparser.addArgument("--output")
            .help("specify an output file; if omitted or '-', conf is generated to stdout")
            .setDefault("-");
        subparser.addArgument("--archive-resolver")
            .help("DNS server IP address which can act as an appropriate resolver for S3 buckets")
            .dest("archiveResolver")
            .setDefault(NginxConfParams.NGINX_ARTIFACT_ARCHIVE_DEFAULT_RESOLVER);

        subparser.addArgument("nginxRoot")
            .help("the root from which nginx should serve any requests not handled by fallout");
    }

    public static void withWriterFromOutputString(String output, Consumer<Writer> consumer) throws IOException
    {
        try (Writer outputWriter = output.equals("-") ?
            new OutputStreamWriter(System.out, StandardCharsets.UTF_8) :
            Files.newBufferedWriter(Paths.get(output), StandardCharsets.UTF_8))
        {
            consumer.accept(outputWriter);
        }
    }

    public static void generateNginxConf(
        Writer output, NginxConfParams nginxConfParams, FalloutConfiguration configuration)
    {
        final Mustache template = new DefaultMustacheFactory().compile("nginx/fallout.nginx.conf.mustache");
        template.execute(output, List.of(
            Map.of("nginxServerName", Exceptions.getUnchecked(
                () -> URI.create(configuration.getExternalUrl()).toURL().getHost())),
            configuration,
            nginxConfParams));
    }

    @Override
    protected void run(Bootstrap<FC> bootstrap,
        Namespace namespace, FC configuration) throws Exception
    {
        withWriterFromOutputString(namespace.getString("output"),
            writer -> generateNginxConf(writer, new NginxConfParams(namespace), configuration));
    }
}
