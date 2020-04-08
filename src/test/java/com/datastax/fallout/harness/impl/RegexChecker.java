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
package com.datastax.fallout.harness.impl;

import java.util.Collection;
import java.util.List;
import java.util.regex.Pattern;

import com.google.auto.service.AutoService;
import com.google.common.collect.ImmutableList;

import com.datastax.fallout.harness.Checker;
import com.datastax.fallout.harness.Operation;
import com.datastax.fallout.ops.Ensemble;
import com.datastax.fallout.ops.PropertyGroup;
import com.datastax.fallout.ops.PropertySpec;
import com.datastax.fallout.ops.PropertySpecBuilder;

@AutoService(Checker.class)
public class RegexChecker extends Checker
{

    static final String prefix = "fallout.checkers.regex.";
    static final String duplicateCharRegex = "(?!.*(.).*\\\\1)";

    static final PropertySpec<String> regexSpec =
        PropertySpecBuilder.createStr(prefix)
            .name("regex")
            .description("Regex to match against")
            .defaultOf(".*")
            .build();

    static final PropertySpec<Boolean> forbidDupeCharsSpec =
        PropertySpecBuilder.createBool(prefix, false)
            .name("forbid.duplicate_characters")
            .description("Forbid duplicate characters in output. Prepends (?!.*(.).*\\\\1) to your regex.")
            .build();

    @Override
    public String prefix()
    {
        return prefix;
    }

    @Override
    public List<PropertySpec> getPropertySpecs()
    {
        return ImmutableList.<PropertySpec>builder()
            .add(regexSpec)
            .add(forbidDupeCharsSpec)
            .build();
    }

    @Override
    public String name()
    {
        return "regex";
    }

    @Override
    public String description()
    {
        return "Checks that the ops emitted by the TextModule to the jepsen log, when concatenated, match a regex.";
    }

    @Override
    public boolean check(Ensemble ensemble, Collection<Operation> history)
    {
        PropertyGroup checkerProperties = getProperties();
        String regex = regexSpec.value(checkerProperties);
        Boolean forbidDuplicateCharacters = forbidDupeCharsSpec.value(checkerProperties);
        if (forbidDuplicateCharacters)
        {
            regex = duplicateCharRegex + regex;
        }

        String textInHistory = history.stream()
            .filter(op -> op.getModule() != null && op.getModule().name().equals("text"))
            .filter(op -> op.getType().equals(Operation.Type.ok))
            .map(op -> op.getValue().toString())
            .reduce("", String::concat);
        Pattern pattern = Pattern.compile(regex);
        logger.info("Pattern is {}", regex);
        logger.info("History is {}", textInHistory);
        return pattern.matcher(textInHistory).matches();
    }
}
