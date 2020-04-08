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

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.ByteStreams;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.fallout.test.utils.WithPersistentTestOutputDir;

import static com.datastax.fallout.util.ShellUtils.escape;
import static com.datastax.fallout.util.ShellUtils.expandVars;
import static com.datastax.fallout.util.ShellUtils.split;
import static org.assertj.core.api.Assertions.assertThat;

// Tests for ShellUtils, initially lifted from https://gist.github.com/raymyers/8077031
public class ShellUtilsTest extends WithPersistentTestOutputDir
{
    private static final Logger logger = LoggerFactory.getLogger(ShellUtilsTest.class);

    private static void assertEscape(String input, String expected)
    {
        assertThat(escape(input)).isEqualTo(expected);
        // Splitting an escaped string should _always_ render a single token.
        assertThat(split(expected)).hasSize(1);
    }

    private String[] run(List<String> command, Map<String, String> extraEnv, Duration timeout)
    {

        logger.info("run: {}", command);

        ProcessBuilder processBuilder = new ProcessBuilder();

        processBuilder
            .command(command)
            .redirectError(ProcessBuilder.Redirect.INHERIT)
            .environment().putAll(extraEnv);

        Process process = Exceptions.getUncheckedIO(processBuilder::start);
        CompletableFuture<String> reader = CompletableFuture.supplyAsync(
            () -> new String(Exceptions.getUncheckedIO(() -> ByteStreams.toByteArray(process.getInputStream())),
                StandardCharsets.UTF_8));

        boolean completed =
            Exceptions.getUninterruptibly(() -> process.waitFor(timeout.getSeconds(), TimeUnit.SECONDS));
        if (!completed)
        {
            process.destroyForcibly();
        }

        assertThat(completed).isTrue();
        assertThat(process.exitValue()).isEqualTo(0);

        return reader.join().split("\n");
    }

    private String[] runScript(List<String> lines, Map<String, String> extraEnv)
    {
        final Path scriptPath = persistentTestOutputDir().resolve("script");

        Exceptions.runUncheckedIO(() -> {
            Files.write(scriptPath, lines, StandardCharsets.UTF_8);
            Files.setPosixFilePermissions(scriptPath, PosixFilePermissions.fromString("rwxr-xr-x"));
        });

        return run(ImmutableList.of(scriptPath.toString()), extraEnv, Duration.ofSeconds(5));
    }

    private String[] showArgs(String args, Map<String, String> extraEnv)
    {
        return runScript(
            ImmutableList.of(
                "#!/bin/bash",
                "function show_args() { for arg in \"$@\"; do echo \"$arg\"; done }",
                String.format("show_args %s", args)),
            extraEnv);
    }

    @Test
    public void split_blank_is_empty()
    {
        assertThat(split("")).isEmpty();
    }

    @Test
    public void split_whitespaces_is_empty()
    {
        assertThat(split("  \t \n")).isEmpty();
    }

    @Test
    public void split_unquoted()
    {
        assertThat(split("a\tbee  cee")).containsExactly("a", "bee", "cee");
    }

    @Test
    public void split_double_quoted()
    {
        assertThat(split("\"hello world\"")).containsExactly("hello world");
    }

    @Test
    public void split_single_quoted()
    {
        assertThat(split("'hello world'")).containsExactly("hello world");
    }

    @Test
    public void split_escaped_double_quotes()
    {
        assertThat(split("\"\\\"hello world\\\"")).containsExactly("\"hello world\"");
    }

    @Test
    public void split_escapes_within_single_quotes_ignores_escapes()
    {
        assertThat(split("'hello \\\" world'")).containsExactly("hello \\\" world");
    }

    @Test
    public void split_adjacent_quoted_tokens_forms_a_single_token()
    {
        assertThat(split("\"foo\"'bar'baz")).containsExactly("foobarbaz");
        assertThat(split("\"three\"' 'four")).containsExactly("three four");
    }

    @Test
    public void split_escaped_whitespace_makes_single_token()
    {
        assertThat(split("three\\ four")).containsExactly("three four");
    }

    @Test
    public void escaping_a_string_with_no_whitespace_or_quotes_has_no_effect()
    {
        assertEscape("foo", "foo");
    }

    @Test
    public void escaping_whitespace_uses_single_quotes()
    {
        assertEscape("foo bar", "'foo bar'");
    }

    @Test
    public void escaping_a_single_quote_uses_double_quote_escaping()
    {
        assertEscape("foo'bar", "foo'\"'\"'bar");
    }

    @Test
    public void escaping_whitespace_and_single_quote_uses_double_quote_escaping()
    {
        assertEscape("foo ' bar", "'foo '\"'\"' bar'");
    }

    @Test
    public void escaping_escaped_double_quotes_uses_single_quotes()
    {
        assertEscape("echo \"foobar\"", "'echo \"foobar\"'");
    }

    @Test
    public void escaping_single_quoted_string_uses_double_quote_escaping()
    {
        assertEscape("echo 'foobar'", "'echo '\"'\"'foobar'\"'\"''");
    }

    @Test
    public void expanding_should_not_quote_its_arguments()
    {
        assertThat(expandVars("foo")).isEqualTo("\"$(eval echo foo)\"");
    }

    @Test
    public void show_args_shows_separate_args()
    {
        assertThat(showArgs("a b 'c' 'd e' 'f ' \"$FOO\"", ImmutableMap.of("FOO", "foo bar")))
            .containsExactly("a", "b", "c", "d e", "f ", "foo bar");
    }

    @Test
    public void escaping_prevents_variable_expansion()
    {
        assertThat(
            showArgs(escape("-Dfoo=$BAR/baz -server"),
                ImmutableMap.of("BAR", "bungo")))
                    .containsExactly("-Dfoo=$BAR/baz -server");
    }

    @Test
    public void expanding_enables_variable_expansion_and_preserves_tokens()
    {
        assertThat(
            showArgs(expandVars(escape("-Dfoo=$BAR/baz -server")),
                ImmutableMap.of("BAR", "bungo")))
                    .containsExactly("-Dfoo=bungo/baz -server");
    }
}
