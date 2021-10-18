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
package com.datastax.fallout.service.resources.server;

import javax.ws.rs.client.Invocation;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import java.io.IOException;
import java.net.URI;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.function.Function;

import com.google.common.base.Strings;
import io.dropwizard.testing.ConfigOverride;
import org.apache.commons.lang3.tuple.Pair;
import org.glassfish.jersey.client.ClientProperties;
import org.glassfish.jersey.client.filter.EncodingFilter;
import org.glassfish.jersey.message.GZipEncoder;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import com.datastax.fallout.TestHelpers;
import com.datastax.fallout.runner.Artifacts;
import com.datastax.fallout.service.cli.GenerateNginxConf.NginxConfParams;
import com.datastax.fallout.service.core.TestRun;
import com.datastax.fallout.service.resources.FalloutAppExtension;
import com.datastax.fallout.service.resources.RestApiBuilder;
import com.datastax.fallout.service.views.FalloutView;
import com.datastax.fallout.test.utils.WithPersistentTestOutputDir;
import com.datastax.fallout.util.Exceptions;
import com.datastax.fallout.util.FileUtils;

import static com.datastax.fallout.assertj.Assertions.assertThat;
import static com.datastax.fallout.service.artifacts.ArtifactServlet.COMPRESSED_RANGE_REQUEST_ERROR_MESSAGE;
import static com.datastax.fallout.service.cli.GenerateNginxConf.NginxConfParams.NGINX_GZIP_MIN_LENGTH;
import static com.datastax.fallout.service.cli.GenerateNginxConf.generateNginxConf;
import static com.datastax.fallout.service.cli.GenerateNginxConf.withWriterFromOutputString;
import static com.datastax.fallout.service.core.Fakes.TEST_USER_EMAIL;
import static javax.ws.rs.core.Response.Status.FORBIDDEN;
import static javax.ws.rs.core.Response.Status.NOT_FOUND;
import static javax.ws.rs.core.Response.Status.OK;
import static javax.ws.rs.core.Response.Status.PARTIAL_CONTENT;
import static org.assertj.core.api.Assumptions.assumeThat;

public class ArtifactResourceTest
{
    public static final String ARTIFACT_CONTENT = "It's full of stars! ";
    public static final String LARGE_ARTIFACT_CONTENT = ARTIFACT_CONTENT.repeat(100);
    public static final MediaType LOG_MEDIATYPE = MediaType.TEXT_PLAIN_TYPE.withCharset("utf-8");

    @Tag("requires-db")
    public static abstract class Tests extends WithPersistentTestOutputDir
    {
        private RestApiBuilder api;
        private TestRun testRun;
        protected static Path rootArtifactPath;
        private Path testRunRootArtifactPath;

        protected abstract RestApiBuilder getRestApiBuilder();

        @BeforeEach
        public void setup()
        {
            api = getRestApiBuilder();
            testRun = new TestRun();
            testRun.setOwner(TEST_USER_EMAIL);
            testRun.setTestName("testName");
            testRun.setTestRunId(UUID.fromString("69A38F36-8A91-4ABB-A2C8-B669189FEFD5"));
            testRunRootArtifactPath = Artifacts.buildTestRunArtifactPath(rootArtifactPath, testRun);
            FileUtils.deleteDir(testRunRootArtifactPath);
            FileUtils.createDirs(testRunRootArtifactPath);
        }

        private URI showTestRunArtifactsUri()
        {
            return FalloutView.uriFor(TestResource.class, "showTestRunArtifacts",
                testRun.getOwner(),
                testRun.getTestName(),
                testRun.getTestRunId());
        }

        private WebTarget createArtifactTarget(String artifactName)
        {
            return api.target(showTestRunArtifactsUri().toString())
                .path(artifactName)
                .property(ClientProperties.FOLLOW_REDIRECTS, false);
        }

        private WebTarget createArtifactServletTarget(String artifactName)
        {
            return api.target("artifacts")
                .path(testRun.getOwner())
                .path(testRun.getTestName())
                .path(testRun.getTestRunId().toString())
                .path(artifactName)
                .property(ClientProperties.FOLLOW_REDIRECTS, false);
        }

        private Response fetchArtifact(String artifactName, boolean decompressGzipResponses,
            Function<String, WebTarget> createTarget,
            Consumer<Invocation.Builder> builderModifier)
        {
            final WebTarget target = createTarget.apply(artifactName);

            if (decompressGzipResponses)
            {
                target.register(EncodingFilter.class);
                target.register(GZipEncoder.class);
            }

            final Invocation.Builder builder = target.request();

            if (!decompressGzipResponses)
            {
                builder.acceptEncoding("gzip");
            }

            builderModifier.accept(builder);

            return builder.get();
        }

        private Response fetchArtifact(String artifactName, Function<String, WebTarget> createTarget)
        {
            return fetchArtifact(artifactName, true, createTarget, ignored -> {});
        }

        private Response fetchArtifact(String artifactName)
        {
            return fetchArtifact(artifactName, false, this::createArtifactTarget, ignored -> {});
        }

        private Response fetchArtifactWithRange(String artifactName, String range)
        {
            return fetchArtifact(artifactName, false, this::createArtifactTarget,
                builder -> builder.header("Range", range));
        }

        private Response fetchAndDecompressArtifact(String artifactName)
        {
            return fetchArtifact(artifactName, true, this::createArtifactTarget, ignored -> {});
        }

        private Response assertThatResponseHasStatusAndContentsMatch(
            Response response, Response.StatusType statusType,
            String expectedContents, MediaType expectedMediaType)
        {
            assertThat(response).hasStatusInfo(statusType);

            assertThat(response).hasMediaType(expectedMediaType);
            assertThat(response.readEntity(String.class))
                .isEqualTo(expectedContents)
                .isNotEmpty();

            return response;
        }

        private Response assertThatResponseIsValidAndContentsMatch(
            Response response, String expectedContents, MediaType expectedMediaType)
        {
            return assertThatResponseHasStatusAndContentsMatch(response, OK, expectedContents, expectedMediaType);
        }

        protected void assertThatResponseIsGzipped(Response response)
        {
            assertThat(response.getHeaderString("content-encoding")).isEqualTo("gzip");
        }

        protected void assertThatResponseIsNotGzipped(Response response)
        {
            assertThat(response.getHeaderString("content-encoding")).isNull();
        }

        protected Response assertThatRequestForAnExistingArtifactSucceeds(
            String artifactName, String artifactContent, MediaType expectedMediaType)
        {
            TestHelpers.createArtifact(testRunRootArtifactPath, artifactName, artifactContent);

            Response response = fetchAndDecompressArtifact(artifactName);
            assertThatResponseIsValidAndContentsMatch(
                response,
                artifactContent,
                expectedMediaType);

            return response;
        }

        protected Response assertThatRangeRequestForAnExistingArtifactSucceeds(
            String artifactName, String artifactContent, MediaType expectedMediaType)
        {
            TestHelpers.createArtifact(testRunRootArtifactPath, artifactName, artifactContent);

            Response response = fetchArtifactWithRange(artifactName, "bytes=5-14");
            assertThatResponseHasStatusAndContentsMatch(
                response,
                PARTIAL_CONTENT,
                artifactContent.substring(5, 15), // range request end indices are inclusive, substring's are exclusive
                expectedMediaType);

            assertThatResponseIsNotGzipped(response);

            return response;
        }

        @Test
        public void a_request_for_an_existing_artifact_succeeds()
        {
            assertThatRequestForAnExistingArtifactSucceeds(
                "monolith.log",
                ARTIFACT_CONTENT,
                LOG_MEDIATYPE);
        }

        @Test
        public void a_request_for_an_existing_artifact_under_artifacts_is_redirected()
        {
            final String artifactName = "monolith.log";
            TestHelpers.createArtifact(testRunRootArtifactPath, artifactName, ARTIFACT_CONTENT);

            Response response = fetchArtifact(artifactName, this::createArtifactServletTarget);

            assertThat(response).hasStatusInfo(Response.Status.FOUND);

            response = fetchArtifact(artifactName, artifactName_ -> createArtifactServletTarget(artifactName_)
                .property(ClientProperties.FOLLOW_REDIRECTS, true));

            assertThatResponseIsValidAndContentsMatch(
                response,
                ARTIFACT_CONTENT,
                LOG_MEDIATYPE);
        }

        @Test
        public void a_request_for_an_artifact_directory_redirects_to_the_root_artifact_location()
        {
            final String artifactDir = "foo/bar/baz";
            FileUtils.createDirs(testRunRootArtifactPath.resolve(artifactDir));

            Response response = fetchArtifact(artifactDir);

            assertThat(response)
                .hasStatusInfo(Response.Status.FOUND)
                .hasLocation(api.target(showTestRunArtifactsUri().toString()).getUri());
        }

        @Test
        public void mime_types_are_as_expected()
        {
            final String artifactBasename = "monolith";
            final List<Pair<String, MediaType>> expectedMimeTypes = List.of(
                Pair.of(".log", MediaType.TEXT_PLAIN_TYPE.withCharset("utf-8")),
                Pair.of(".csv", MediaType.valueOf("text/csv")),
                Pair.of(".xml", MediaType.APPLICATION_XML_TYPE),
                Pair.of(".json", MediaType.APPLICATION_JSON_TYPE));

            for (Pair<String, MediaType> expectedMimeType : expectedMimeTypes)
            {
                String artifactName = artifactBasename + expectedMimeType.getLeft();
                MediaType mediaType = expectedMimeType.getRight();

                assertThatRequestForAnExistingArtifactSucceeds(artifactName, ARTIFACT_CONTENT, mediaType);
            }
        }

        @Test
        public void a_request_for_a_nonexistent_artifact_fails()
        {
            assertThat(fetchArtifact("nothing-doing.csv")).hasStatusInfo(NOT_FOUND);
        }

        @Test
        public void a_request_for_an_artifact_that_has_been_compressed_responds_with_pre_compressed_output()
        {
            final String artifactName = "monolith.log";
            final String compressedArtifactName = artifactName + ".gz";
            final String compressedArtifactContent = "It's full of neutron stars!";
            TestHelpers.createArtifact(testRunRootArtifactPath, compressedArtifactName, compressedArtifactContent);

            final Response response = assertThatResponseIsValidAndContentsMatch(
                fetchArtifact(artifactName),
                compressedArtifactContent,
                LOG_MEDIATYPE);

            assertThatResponseIsGzipped(response);
        }

        @Test
        public void a_range_request_on_an_uncompressed_artifact_returns_uncompressed_output()
        {
            assertThatRangeRequestForAnExistingArtifactSucceeds("monolith.log", "It's full of stars!", LOG_MEDIATYPE);
        }

        @Test
        public void a_range_request_on_a_compressed_artifact_fails()
        {
            final String artifactName = "monolith.log";
            final String compressedArtifactName = artifactName + ".gz";
            final String compressedArtifactContent = "It's full of neutron stars!";
            TestHelpers.createArtifact(testRunRootArtifactPath, compressedArtifactName, compressedArtifactContent);

            Response response = fetchArtifactWithRange(artifactName, "bytes=5-14");
            assertThat(response).hasStatusInfo(FORBIDDEN);
            assertThat(response.readEntity(String.class)).contains(COMPRESSED_RANGE_REQUEST_ERROR_MESSAGE);
        }
    }

    /** Start an nginx proxy and test artifact retrieval using that.
     *
     * <p>Note that this test produces an nginx alert on debian distros; see the comment on the
     * error_log setting in src/main/resources/nginx/fallout.nginx.conf.mustache for details. */
    public static class UsingNginxProxy extends Tests
    {
        @RegisterExtension
        public static final FalloutAppExtension FALLOUT_SERVICE = new FalloutAppExtension(
            ConfigOverride.config("useNginxToServeArtifacts", "true"));

        @RegisterExtension
        public final FalloutAppExtension.FalloutServiceResetExtension FALLOUT_SERVICE_RESET =
            FALLOUT_SERVICE.resetExtension();

        private static final int NGINX_LISTEN_PORT = 9080;
        private static final Path NGINX_ROOT = Paths.get("etc/nginx/html").toAbsolutePath();

        private static Optional<Process> nginx = Optional.empty();

        @BeforeAll
        public static void setRootArtifactPath()
        {
            Tests.rootArtifactPath = FALLOUT_SERVICE.getArtifactPath();
        }

        @BeforeAll
        public static void startNginxIfAvailable() throws IOException
        {
            final Path nginxConf = persistentTestClassOutputDir()
                .resolve("nginx").resolve("unit-test.nginx.conf").toAbsolutePath();

            Process whichNginx = new ProcessBuilder("which", "nginx").start();
            if (Exceptions.getUninterruptibly(whichNginx::waitFor) != 0)
            {
                return;
            }

            assertThat(nginxConf.getParent().toFile().mkdirs()).isTrue();

            withWriterFromOutputString(nginxConf.toString(), writer -> generateNginxConf(
                writer,
                new NginxConfParams(true, NGINX_LISTEN_PORT, NGINX_ROOT),
                FALLOUT_SERVICE.getConfiguration()));

            nginx = Optional.of(new ProcessBuilder(
                "nginx",
                "-p", nginxConf.getParent().toString(),
                "-c", nginxConf.toString())
                    .inheritIO()
                    .start());
        }

        @AfterAll
        public static void stopNginx()
        {
            nginx.ifPresent(nginx_ -> {
                nginx_.destroy();
                Exceptions.getUninterruptibly(nginx_::waitFor);
            });
            nginx = Optional.empty();
        }

        private URI getNginxUri()
        {
            assumeThat(nginx)
                .withFailMessage("Nginx not available; skipping test")
                .isPresent();

            assertThat(nginx.get().isAlive())
                .withFailMessage("Nginx is not running")
                .isTrue();

            return Exceptions.getUnchecked(() -> new URL("http", "localhost", NGINX_LISTEN_PORT, "/").toURI());
        }

        @Override
        protected RestApiBuilder getRestApiBuilder()
        {
            return FALLOUT_SERVICE_RESET.userApi().connectTo(getNginxUri());
        }

        @Test
        public void a_request_for_a_small_compressible_artifact_is_not_compressed()
        {
            Response response = assertThatRequestForAnExistingArtifactSucceeds(
                "monolith.log",
                Strings.repeat(ARTIFACT_CONTENT, NGINX_GZIP_MIN_LENGTH / ARTIFACT_CONTENT.length() - 1),
                LOG_MEDIATYPE);

            assertThatResponseIsNotGzipped(response);
        }

        @Test
        public void a_request_for_a_large_compressible_artifact_is_compressed()
        {
            Response response = assertThatRequestForAnExistingArtifactSucceeds(
                "monolith.log",
                Strings.repeat(ARTIFACT_CONTENT, NGINX_GZIP_MIN_LENGTH / ARTIFACT_CONTENT.length() + 1),
                LOG_MEDIATYPE);

            assertThatResponseIsGzipped(response);
        }

        @Test
        public void a_request_for_a_large_incompressible_artifact_is_not_compressed()
        {
            Response response = assertThatRequestForAnExistingArtifactSucceeds(
                "monolith.jpg",
                Strings.repeat(ARTIFACT_CONTENT, NGINX_GZIP_MIN_LENGTH / ARTIFACT_CONTENT.length() + 1),
                MediaType.valueOf("image/jpeg"));

            assertThatResponseIsNotGzipped(response);
        }

        @Test
        public void a_range_request_on_a_large_compressible_artifact_is_not_compressed()
        {
            assertThatRangeRequestForAnExistingArtifactSucceeds("monolith.log",
                Strings.repeat(ARTIFACT_CONTENT, NGINX_GZIP_MIN_LENGTH / ARTIFACT_CONTENT.length() + 1),
                LOG_MEDIATYPE);
        }
    }

    public static class UsingFalloutService extends Tests
    {
        @RegisterExtension
        public static final FalloutAppExtension FALLOUT_SERVICE = new FalloutAppExtension();

        @RegisterExtension
        public final FalloutAppExtension.FalloutServiceResetExtension FALLOUT_SERVICE_RESET =
            FALLOUT_SERVICE.resetExtension();

        @BeforeAll
        public static void setRootArtifactPath()
        {
            Tests.rootArtifactPath = FALLOUT_SERVICE.getArtifactPath();
        }

        @Override
        protected RestApiBuilder getRestApiBuilder()
        {
            return FALLOUT_SERVICE_RESET.userApi();
        }
    }
}
