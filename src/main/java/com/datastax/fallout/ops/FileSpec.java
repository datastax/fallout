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
package com.datastax.fallout.ops;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import org.slf4j.Logger;

import com.datastax.fallout.components.common.spec.GitClone;
import com.datastax.fallout.exceptions.InvalidConfigurationException;
import com.datastax.fallout.harness.ActiveTestRunBuilder;
import com.datastax.fallout.ops.commands.CommandExecutor;
import com.datastax.fallout.ops.commands.FullyBufferedNodeResponse;
import com.datastax.fallout.ops.utils.FileUtils;
import com.datastax.fallout.util.Exceptions;
import com.datastax.fallout.util.JsonUtils;
import com.datastax.fallout.util.YamlUtils;

public abstract class FileSpec
{
    private static final Map<String, Class<? extends FileSpec>> keyToType = Map.of(
        "data", DataFileSpec.class,
        "yaml", YamlFileSpec.class,
        "json", JsonFileSpec.class,
        "url", UrlFileSpec.class,
        "git", GitFileSpec.class);

    protected final String path;

    FileSpec(String path)
    {
        if (path == null)
        {
            throw new PropertySpec.ValidationException("FileSpec is missing a file name!");
        }
        this.path = path.startsWith("/") ? path.substring(1) : path;
    }

    public boolean createLocalFile(Logger logger, CommandExecutor commandExecutor, Path fullFilePath,
        ActiveTestRunBuilder.ValidationResult validationResult)
    {
        if (Files.exists(fullFilePath))
        {
            logger.info("File at {} already exists", fullFilePath);
            return true;
        }

        logger.info("Attempting to create file at {} ", fullFilePath);
        try
        {
            return createLocalFileImpl(logger, commandExecutor, fullFilePath);
        }
        catch (IOException e)
        {
            String errorMessage = String.format("Error while creating local file %s", fullFilePath);
            validationResult.addError(errorMessage + ": " + e.getMessage());
            logger.error(errorMessage, e);
            return false;
        }
    }

    public abstract boolean createLocalFileImpl(Logger logger, CommandExecutor commandExecutor, Path fullFilePath)
        throws IOException;

    public abstract boolean shadeLocalFile(NodeGroup nodeGroup, Path fullLocalPath);

    public String getPath()
    {
        return path;
    }

    /** Whether this filespec would satisfy the <code>ref</code> in <code>&lt;&lt;file:ref&gt;&gt;</code>*/
    public boolean matchesManagedFileRef(String managedFileRef)
    {
        return path.startsWith(managedFileRef);
    }

    public Node.ExistsCheckType getExistsCheckType()
    {
        return Node.ExistsCheckType.FILE;
    }

    public Path getFullPath(Path rootFilePath)
    {
        return rootFilePath.resolve(path);
    }

    private static void maybeCreateParentDir(Path file)
    {
        FileUtils.createDirs(file.getParent());
    }

    public static FileSpec fromMap(Map<String, Object> map)
    {
        Map<String, Object> mapCopy = new HashMap<>(map);
        String path = (String) mapCopy.getOrDefault("path", "");
        mapCopy.remove("path");

        if (mapCopy.size() != 1)
        {
            throw new InvalidConfigurationException(
                "Incorrect file spec type. There should be exactly one file spec type per item in the files list.");
        }

        String type = mapCopy.keySet().iterator().next();

        if (!keyToType.containsKey(type))
        {
            throw new InvalidConfigurationException(
                String.format("File '%s' has an unknown file type '%s'", path, type));
        }

        try
        {
            return keyToType.get(type)
                .getConstructor(String.class, Object.class)
                .newInstance(path, mapCopy.get(type));
        }
        catch (InvocationTargetException e)
        {
            // report cast error from init
            throw new InvalidConfigurationException(e.getCause());
        }
        catch (InstantiationException | NoSuchMethodException | IllegalAccessException e)
        {
            throw new InvalidConfigurationException(e);
        }
    }

    public static abstract class ContentBasedFile extends FileSpec
    {
        ContentBasedFile(String path)
        {
            super(path);
        }

        @Override
        public boolean createLocalFileImpl(Logger logger, CommandExecutor commandExecutor, Path fullFilePath)
            throws IOException
        {
            maybeCreateParentDir(fullFilePath);
            Files.writeString(fullFilePath, getFileContent());
            return true;
        }

        @Override
        public boolean shadeLocalFile(NodeGroup nodeGroup, Path fullLocalPath)
        {
            return true; // no-op so tests have a full copy of the file content as an artifact
        }

        protected abstract String getFileContent();
    }

    public static class DataFileSpec extends ContentBasedFile
    {
        private final String data;

        public DataFileSpec(String path, Object val)
        {
            super(path);
            try
            {
                data = (String) val;
            }
            catch (ClassCastException e)
            {
                throw new InvalidConfigurationException(
                    String.format("Data file '%s' expects value to be a string, not '%s'", path, val), e);
            }
        }

        @Override
        protected String getFileContent()
        {
            return data;
        }
    }

    public static class YamlFileSpec extends ContentBasedFile
    {
        Object yaml;

        public YamlFileSpec(String path, Object val)
        {
            super(path);
            yaml = val;
        }

        @Override
        protected String getFileContent()
        {
            return YamlUtils.dumpYaml(yaml);
        }
    }

    public static class JsonFileSpec extends ContentBasedFile
    {
        Object json;

        public JsonFileSpec(String path, Object val)
        {
            super(path);
            json = val;
        }

        @Override
        protected String getFileContent()
        {
            return JsonUtils.toJson(json);
        }
    }

    public static class UrlFileSpec extends ContentBasedFile
    {
        public static final int DOWNLOAD_FROM_URL_TIMEOUT_MILLIS = 60000;

        URL url;

        public UrlFileSpec(String path, Object val)
        {
            super(path);
            try
            {
                url = new URL(val.toString());
            }
            catch (MalformedURLException e)
            {
                throw new InvalidConfigurationException(
                    String.format("Url file '%s' expects value to be a valid url, not '%s'", path, val), e);
            }
        }

        public static String getFileContent(URL url)
        {
            return Exceptions.getUncheckedIO(() -> {
                URLConnection urlConnection = url.openConnection();
                urlConnection.setConnectTimeout(DOWNLOAD_FROM_URL_TIMEOUT_MILLIS);
                urlConnection.setReadTimeout(DOWNLOAD_FROM_URL_TIMEOUT_MILLIS);

                try (InputStream inputStream = urlConnection.getInputStream())
                {
                    return new String(inputStream.readAllBytes(), StandardCharsets.UTF_8);
                }
            });
        }

        @Override
        protected String getFileContent()
        {
            return getFileContent(url);
        }
    }

    public static class GitFileSpec extends FileSpec
    {
        private final String repo;
        private final Optional<String> branch;

        public GitFileSpec(String path, Object val)
        {
            super(path);
            Map<String, String> gitFileSpecMap;
            try
            {
                gitFileSpecMap = (Map<String, String>) val;
            }
            catch (ClassCastException e)
            {
                throw new InvalidConfigurationException(
                    "GitFileSpec expects value to be a map containing keys repo, and optionally branch.", e);
            }
            if (!gitFileSpecMap.containsKey("repo"))
            {
                throw new PropertySpec.ValidationException("GitFileSpec is missing repo.");
            }
            repo = GitClone.expandShortRepo(gitFileSpecMap.get("repo"));
            branch = Optional.ofNullable(gitFileSpecMap.get("branch"));
            // Ensure repo name can be parsed
            GitClone.parseNameOfRepo(repo);
        }

        public static FileSpec create(String repo, String branch)
        {
            return new GitFileSpec("", Map.of(
                "repo", repo,
                "branch", branch
            ));
        }

        public String getRepoName()
        {
            return GitClone.parseNameOfRepo(repo);
        }

        @Override
        public boolean createLocalFileImpl(Logger logger, CommandExecutor commandExecutor, Path fullFilePath)
            throws IOException
        {
            return commandExecutor
                .local(logger, getCloneStatement(fullFilePath)).execute().waitForSuccess();
        }

        @Override
        public boolean shadeLocalFile(NodeGroup nodeGroup, Path fullLocalPath)
        {
            String repoName = getRepoName();
            FullyBufferedNodeResponse getRevision = nodeGroup.getProvisioner().getCommandExecutor()
                .local(nodeGroup.logger(), "git rev-parse HEAD")
                .workingDirectory(fullLocalPath)
                .execute()
                .buffered();

            if (!getRevision.waitForSuccess())
            {
                throw new RuntimeException(
                    String.format("Could not get revision from managed git clone: %s %s", getPath(), repoName));
            }

            String revision = getRevision.getStdout();
            String gitInfo = String.format("%s\n%s", repo, revision);

            String pathRepoSuffix = path.isEmpty() ?
                String.format("_%s", repoName) :
                String.format("_%s_%s", getPath(), repoName);

            Path gitInfoFile = fullLocalPath.getParent().resolve(String.format("git_info%s.txt", pathRepoSuffix));

            Exceptions.runUncheckedIO(() -> {
                maybeCreateParentDir(gitInfoFile);
                Files.writeString(gitInfoFile, gitInfo);
                FileUtils.deleteDir(fullLocalPath);
            });

            return true;
        }

        private String getCloneStatement(Path fullFilePath)
        {
            return GitClone.statement(repo, fullFilePath.toString(), branch, true);
        }

        @Override
        public String getPath()
        {
            return cloneDir();
        }

        private String cloneDir()
        {
            return Paths.get(path, GitClone.parseNameOfRepo(repo)).toString();
        }

        @Override
        public Path getFullPath(Path rootFilePath)
        {
            return rootFilePath.resolve(cloneDir());
        }

        @Override
        public boolean matchesManagedFileRef(String managedFileRef)
        {
            return managedFileRef.startsWith(getPath());
        }

        @Override
        public Node.ExistsCheckType getExistsCheckType()
        {
            return Node.ExistsCheckType.DIRECTORY;
        }
    }
}
