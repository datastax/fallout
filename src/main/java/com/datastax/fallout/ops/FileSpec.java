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
import java.util.concurrent.CompletableFuture;

import com.google.common.collect.ImmutableMap;

import com.datastax.fallout.exceptions.InvalidConfigurationException;
import com.datastax.fallout.harness.specs.GitClone;
import com.datastax.fallout.ops.commands.FullyBufferedNodeResponse;
import com.datastax.fallout.util.Exceptions;
import com.datastax.fallout.util.YamlUtils;

public abstract class FileSpec
{
    private static final Map<String, Class<? extends FileSpec>> keyToType = ImmutableMap.of(
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

    public abstract boolean createLocalFile(Path fullFilePath, NodeGroup nodeGroup) throws IOException;

    public abstract CompletableFuture<Boolean> createRemoteAndShadowFile(Path fullRemotePath, Path fullLocalPath,
        NodeGroup nodeGroup);

    public abstract CompletableFuture<Boolean> createShadowFile(Path fullRemotePath, Path fullLocalPath,
        NodeGroup nodeGroup);

    public String getPath()
    {
        return path;
    }

    public Node.ExistsCheckType getExistsCheckType()
    {
        return Node.ExistsCheckType.FILE;
    }

    public Path getFullPath(Path rootFilePath)
    {
        return rootFilePath.resolve(path);
    }

    private static void maybeCreateParentDir(Path file) throws IOException
    {
        Files.createDirectories(file.getParent());
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
        public boolean createLocalFile(Path fullFilePath, NodeGroup nodeGroup) throws IOException
        {
            maybeCreateParentDir(fullFilePath);
            Files.writeString(fullFilePath, getFileContent());
            return true;
        }

        @Override
        public CompletableFuture<Boolean> createRemoteAndShadowFile(Path fullRemotePath, Path fullLocalPath,
            NodeGroup nodeGroup)
        {
            String fileContent = getFileContent();
            return nodeGroup.put(fileContent, fullRemotePath.toString(), 0755)
                .thenApplyAsync(remoteWritten -> {
                    if (!remoteWritten)
                    {
                        nodeGroup.logger().error(String.format("Failed to create remote file %s", path));
                        return false;
                    }
                    try
                    {
                        nodeGroup.logger().info(String.format("Writing shadow file %s to %s", path, fullLocalPath));
                        maybeCreateParentDir(fullLocalPath);
                        Files.writeString(fullLocalPath, fileContent);
                        return true;
                    }
                    catch (IOException e)
                    {
                        nodeGroup.logger().error(String.format("Error while writing shadow file %s", path), e);
                        return false;
                    }
                });
        }

        @Override
        public CompletableFuture<Boolean> createShadowFile(Path fullFilePath, Path fullLocalPath, NodeGroup nodeGroup)
        {
            return nodeGroup.get(fullFilePath.toString(), fullLocalPath, false);
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
            return Utils.json(json);
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

        public String getRepoName()
        {
            return GitClone.parseNameOfRepo(repo);
        }

        @Override
        public boolean createLocalFile(Path fullFilePath, NodeGroup nodeGroup) throws IOException
        {
            return nodeGroup.getProvisioner().getCommandExecutor()
                .executeLocally(nodeGroup.logger(), getCloneStatement(fullFilePath)).waitForSuccess();
        }

        @Override
        public CompletableFuture<Boolean> createRemoteAndShadowFile(Path fullRemotePath, Path fullLocalPath,
            NodeGroup nodeGroup)
        {
            return CompletableFuture.supplyAsync(() -> nodeGroup.waitForSuccess(getCloneStatement(fullRemotePath)))
                .thenComposeAsync(remoteWritten -> {
                    if (!remoteWritten)
                    {
                        nodeGroup.logger().error(String.format("Failed to clone %s to %s", repo, fullRemotePath));
                        return CompletableFuture.completedFuture(false);
                    }
                    return createShadowFile(fullRemotePath, fullLocalPath, nodeGroup);
                });
        }

        @Override
        public CompletableFuture<Boolean> createShadowFile(Path fullRemotePath, Path fullLocalPath, NodeGroup nodeGroup)
        {
            return CompletableFuture.supplyAsync(() -> {
                String repoName = getRepoName();
                String getRevisionCommand = String.format("cd %s && git rev-parse HEAD", fullRemotePath);
                FullyBufferedNodeResponse getRevision = nodeGroup.getNodes().get(0).executeBuffered(getRevisionCommand);
                if (!getRevision.waitForSuccess())
                {
                    throw new RuntimeException(
                        String.format("Could not get revision from managed git clone: %s %s", path, repoName));
                }

                String revision = getRevision.getStdout();
                String gitInfo = String.format("%s\n%s", repo, revision);

                String pathRepoSuffix = path.isEmpty() ?
                    String.format("_%s", repoName) :
                    String.format("_%s_%s", path, repoName);
                Path gitInfoFile = fullLocalPath.resolve(String.format("git_info%s.txt", pathRepoSuffix));

                Exceptions.runUncheckedIO(() -> {
                    maybeCreateParentDir(gitInfoFile);
                    Files.writeString(gitInfoFile, gitInfo);
                });
                return true;
            });
        }

        private String getCloneStatement(Path fullFilePath)
        {
            return GitClone.statement(repo, fullFilePath.toString(), branch, true);
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
        public Node.ExistsCheckType getExistsCheckType()
        {
            return Node.ExistsCheckType.DIRECTORY;
        }
    }
}
