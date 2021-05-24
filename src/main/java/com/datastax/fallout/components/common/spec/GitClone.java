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
package com.datastax.fallout.components.common.spec;

import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.datastax.fallout.ops.HasProperties;
import com.datastax.fallout.ops.PropertySpec;
import com.datastax.fallout.ops.PropertySpecBuilder;

/**
 * This class provides the required {@link com.datastax.fallout.ops.PropertySpec} for a Git repository to be cloned.  It
 * contains the following specs:
 *
 * <ul>
 * <li>The git repository URL where source code can be found</li>
 * <li>The branch to checkout</li>
 * </ul>
 */
public class GitClone
{
    private final PropertySpec<String> gitRepository;
    private final PropertySpec<String> gitBranch;

    private static final Pattern GIT_SHA_PATTERN = Pattern.compile("^[0-9A-Fa-f]{7,40}$");
    private static final Pattern SHORT_REPO_WITHOUT_ORG_PATTERN = Pattern.compile("^[\\w-]+(\\.git)*$");
    private static final Pattern SHORT_REPO_WITH_ORG_PATTERN = Pattern.compile("^[\\w-]+/[\\w-]+(\\.git)*$");
    private static final Pattern REPO_NAME_PATTERN = Pattern.compile(".+/(.+)\\.git");
    private static final String GIT_SSH_CLONE_PREFIX = "git@github.com:";

    public GitClone(String prefix, String defaultRepo, String defaultBranch)
    {
        gitRepository = gitRepositoryBuilder(prefix, "", defaultRepo).build();
        gitBranch = gitBranchBuilder(prefix, "", defaultBranch).build();
    }

    public GitClone(String prefix, String name, String defaultRepo, String defaultBranch)
    {
        gitRepository = gitRepositoryBuilder(prefix, name, defaultRepo).build();
        gitBranch = gitBranchBuilder(prefix, name, defaultBranch).build();
    }

    public <T> GitClone(String prefix, String defaultRepo, String defaultBranch,
        PropertySpec<T> dependsOnSpec, T dependsOnValue)
    {
        gitRepository = gitRepositoryBuilder(prefix, "", defaultRepo)
            .dependsOn(dependsOnSpec, dependsOnValue)
            .build();
        gitBranch = gitBranchBuilder(prefix, "", defaultBranch)
            .dependsOn(dependsOnSpec, dependsOnValue)
            .build();
    }

    public <T> GitClone(String prefix, String name, String defaultRepo, String defaultBranch,
        PropertySpec<T> dependsOnSpec, T dependsOnValue)
    {
        gitRepository = gitRepositoryBuilder(prefix, name, defaultRepo)
            .dependsOn(dependsOnSpec, dependsOnValue)
            .build();
        gitBranch = gitBranchBuilder(prefix, name, defaultBranch)
            .dependsOn(dependsOnSpec, dependsOnValue)
            .build();
    }

    public <T> GitClone(Supplier<String> prefix, String name, String defaultRepo, String defaultBranch,
        PropertySpec<T> dependsOnSpec, T dependsOnValue)
    {
        gitRepository = gitRepositoryBuilder(prefix.get(), name, defaultRepo)
            .dependsOn(dependsOnSpec, dependsOnValue)
            .runtimePrefix(prefix)
            .build();
        gitBranch = gitBranchBuilder(prefix.get(), name, defaultBranch)
            .dependsOn(dependsOnSpec, dependsOnValue)
            .runtimePrefix(prefix)
            .build();
    }

    private static PropertySpecBuilder<String> gitRepositoryBuilder(String prefix, String name, String defaultRepo)
    {
        return PropertySpecBuilder.createStr(prefix)
            .name(buildNameSpec(name, "git.repository"))
            .description("The git repository where the source code can be found")
            .defaultOf(defaultRepo);
    }

    private static PropertySpecBuilder<String> gitBranchBuilder(String prefix, String name, String defaultBranch)
    {
        return PropertySpecBuilder.createStr(prefix)
            .name(buildNameSpec(name, "git.branch"))
            .description("The git branch to checkout (can also be a tag or a SHA 7-40 characters long)")
            .defaultOf(defaultBranch);
    }

    private static String buildNameSpec(String specificName, String generalName)
    {
        return specificName.isEmpty() ? generalName : String.join(".", specificName, generalName);
    }

    public PropertySpec<String> getGitBranch()
    {
        return gitBranch;
    }

    public PropertySpec<String> getGitRepository()
    {
        return gitRepository;
    }

    public Collection<PropertySpec<String>> getSpecs()
    {
        return List.of(gitRepository, gitBranch);
    }

    public String statement(HasProperties propertyObject, String cloneDirectory)
    {
        return statement(propertyObject, cloneDirectory, true);
    }

    public String statement(HasProperties propertyObject, String cloneDirectory, boolean shallow)
    {
        String repository = gitRepository.value(propertyObject);
        String branch = gitBranch.value(propertyObject);
        return statement(repository, cloneDirectory, Optional.of(branch), shallow);
    }

    public static String statement(String repository, String cloneDirectory)
    {
        return statement(repository, cloneDirectory, Optional.empty(), true);
    }

    public static String statement(String repository, String cloneDirectory, Optional<String> optionalBranch,
        boolean shallow)
    {
        repository = expandShortRepo(repository);
        // Check if branch is actually a SHA
        if (optionalBranch.isPresent() && GIT_SHA_PATTERN.matcher(optionalBranch.get()).matches())
        {
            // Since we are checking out a SHA, we cannot do a shallow clone.
            // Also, we have to perform a separate checkout command.
            String command = String.format("git clone %s %s; cd %s; git checkout %s",
                repository, cloneDirectory, cloneDirectory, optionalBranch.get());

            return command;
        }

        String depth = shallow ? " --depth=1" : "";
        String branchArg = optionalBranch.isPresent() ? String.format("--branch %s", optionalBranch.get()) : "";

        return String
            .format("git clone %s %s %s %s; cd %s", depth, repository, branchArg, cloneDirectory, cloneDirectory);
    }

    public String getRepo(HasProperties properties)
    {
        return gitRepository.value(properties);
    }

    public String getBranch(HasProperties properties)
    {
        return gitBranch.value(properties);
    }

    public static String expandShortRepo(String shortRepo)
    {
        String result = shortRepo;
        if (SHORT_REPO_WITHOUT_ORG_PATTERN.matcher(shortRepo).matches())
        {
            result = String.format("%s%s/%s", GIT_SSH_CLONE_PREFIX, "riptano", shortRepo);
        }
        else if (SHORT_REPO_WITH_ORG_PATTERN.matcher(shortRepo).matches())
        {
            result = String.format("%s%s", GIT_SSH_CLONE_PREFIX, shortRepo);
        }
        if (!result.endsWith(".git"))
        {
            result += ".git";
        }
        return result;
    }

    public static String parseNameOfRepo(String repo)
    {
        Matcher m = REPO_NAME_PATTERN.matcher(repo);
        if (!m.matches())
        {
            throw new RuntimeException(
                String.format("Could not parse repo name from %s using regex: %s", repo, REPO_NAME_PATTERN));
        }
        return m.group(1);
    }
}
