/*
 * Copyright 2022 DataStax, Inc.
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
package com.datastax.fallout.service.artifacts;

import java.io.FileInputStream;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompletedMultipartUpload;
import software.amazon.awssdk.services.s3.model.CompletedPart;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.UploadPartRequest;
import software.amazon.awssdk.services.s3.presigner.S3Presigner;
import software.amazon.awssdk.services.s3.presigner.model.GetObjectPresignRequest;

import com.datastax.fallout.runner.Artifacts;
import com.datastax.fallout.service.core.PerformanceReport;
import com.datastax.fallout.service.core.TestRun;
import com.datastax.fallout.util.Exceptions;
import com.datastax.fallout.util.FileUtils;

/**
 * Handles upload/download from a remote artifact archive as well as providing links to artifacts within the archive.
 */
public abstract class ArtifactArchive
{
    protected final Path rootArtifactLocation;

    ArtifactArchive(String rootArtifactLocation)
    {
        this.rootArtifactLocation = Paths.get(rootArtifactLocation);
    }

    public abstract URL getArtifactUrl(Path artifact);

    public abstract void uploadTestRun(TestRun testRun) throws IOException;

    public abstract void uploadReport(PerformanceReport report) throws IOException;

    protected abstract void downloadArtifacts(TestRun testRun) throws IOException;

    public void ensureLocalArtifacts(TestRun testRun) throws IOException
    {
        Path local = Artifacts.buildTestRunArtifactPath(rootArtifactLocation, testRun);
        if (!Files.exists(local))
        {
            downloadArtifacts(testRun);
        }
    }

    public static class S3ArtifactArchive extends ArtifactArchive
    {
        private static final Logger logger = LoggerFactory.getLogger(S3ArtifactArchive.class);

        private static final Region ARCHIVE_REGION = Region.US_WEST_2;
        private static final Duration PRESIGNED_REQUEST_DURATION = Duration.ofSeconds(5);
        private static final Long MULTIPART_UPLOAD_THRESHOLD = 100000000L; // 100MB is recommended

        private final String bucket;
        private final S3Presigner presigner;
        private final S3Client s3Client;

        public S3ArtifactArchive(String rootArtifactLocation, String bucket)
        {
            super(rootArtifactLocation);
            this.bucket = bucket;
            //InstanceProfileCredentialsProvider credentialsProvider = InstanceProfileCredentialsProvider.create();
            this.presigner = S3Presigner.builder()
                .region(ARCHIVE_REGION)
                //.credentialsProvider(credentialsProvider)
                .build();
            this.s3Client = S3Client.builder()
                .region(ARCHIVE_REGION)
                //.credentialsProvider(credentialsProvider)
                .build();
        }

        @Override
        public URL getArtifactUrl(Path artifact)
        {
            GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                .bucket(bucket)
                .key(artifact.toString())
                .build();

            GetObjectPresignRequest getObjectPresignRequest = GetObjectPresignRequest.builder()
                .signatureDuration(PRESIGNED_REQUEST_DURATION)
                .getObjectRequest(getObjectRequest)
                .build();

            return presigner.presignGetObject(getObjectPresignRequest).url();
        }

        private String artifactKey(TestRun testRun, String artifact)
        {
            return String.format("%s/%s", testRun.getArchiveKey(), artifact);
        }

        @Override
        protected void downloadArtifacts(TestRun testRun) throws IOException
        {
            Map<String, Long> artifacts = testRun.getArtifacts();
            logger.info(
                "Downloading test run to archive: {} - {} artifacts - {}", testRun.getShortName(), artifacts.size(),
                testRun.getSizeOnDisk());
            for (var artifact : artifacts.keySet())
            {
                Path localArtifactPath = Artifacts.buildTestRunArtifactPath(rootArtifactLocation, testRun)
                    .resolve(artifact);
                String artifactKey = artifactKey(testRun, artifact);
                try
                {
                    getObject(localArtifactPath, artifactKey);
                }
                catch (IOException e)
                {
                    logger.error("IOException while downloading archived artifact: {} - {}\n",
                        testRun.getShortName(), artifact, e);
                    throw e;
                }
            }
        }

        private void getObject(Path localArtifactPath, String artifactKey) throws IOException
        {
            GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                .bucket(bucket)
                .key(artifactKey)
                .build();
            FileUtils.createDirs(localArtifactPath.getParent());
            Files.copy(s3Client.getObject(getObjectRequest), localArtifactPath);
        }

        private void uploadArtifacts(Map<String, Long> artifactSizeMap, String artifactSetName,
            Function<String, Path> localArtifactPathProducer,
            Function<String, String> artifactKeyProducer) throws IOException
        {
            for (var entry : artifactSizeMap.entrySet())
            {
                String artifact = entry.getKey();
                Long size = entry.getValue();
                Path localArtifactPath = localArtifactPathProducer.apply(artifact);
                String artifactKey = artifactKeyProducer.apply(artifact);
                if (size >= MULTIPART_UPLOAD_THRESHOLD)
                {
                    try
                    {
                        multipartObjectUpload(localArtifactPath, artifactKey);
                    }
                    catch (IOException e)
                    {
                        logger.error("IOException while archiving artifact: {} - {}\n",
                            artifactSetName, artifact, e);
                        throw e;
                    }
                }
                else
                {
                    objectUpload(localArtifactPath, artifactKey);
                }
            }
        }

        @Override
        public void uploadTestRun(TestRun testRun) throws IOException
        {
            testRun.updateArtifactsIfNeeded(
                () -> Exceptions.getUncheckedIO(() -> Artifacts.findTestRunArtifacts(rootArtifactLocation, testRun)));

            Map<String, Long> artifacts = testRun.getArtifacts();
            logger.info(
                "Uploading test run to archive: {} - {} artifacts - {}", testRun.getShortName(), artifacts.size(),
                testRun.getSizeOnDisk());

            uploadArtifacts(
                artifacts,
                testRun.getShortName(),
                (artifact) -> Artifacts.buildTestRunArtifactPath(rootArtifactLocation, testRun).resolve(artifact),
                (artifact) -> artifactKey(testRun, artifact));
        }

        @Override
        public void uploadReport(PerformanceReport report) throws IOException
        {
            Path reportDir = rootArtifactLocation.resolve(Paths.get(report.getReportArtifact()).getParent());
            Map<String, Long> artifacts = Artifacts.findTestRunArtifacts(reportDir);
            logger.info("Uploading performance report to archive: {} - {} artifacts {}",
                report.getReportName(), artifacts.size(), reportDir);
            uploadArtifacts(
                artifacts,
                report.getReportName(),
                reportDir::resolve,
                (artifact) -> String.format("performance_reports/%s/%s/%s",
                    report.getEmail(), report.getReportGuid(), artifact));
        }

        private void multipartObjectUpload(Path localArtifactPath, String artifactKey) throws IOException
        {
            CreateMultipartUploadRequest createMultipartUploadRequest = CreateMultipartUploadRequest.builder()
                .bucket(bucket)
                .key(artifactKey)
                .build();

            CreateMultipartUploadResponse response = s3Client.createMultipartUpload(createMultipartUploadRequest);
            String uploadId = response.uploadId();

            int part = 1;
            byte[] byteArr = new byte[MULTIPART_UPLOAD_THRESHOLD.intValue()];
            var is = new FileInputStream(localArtifactPath.toFile());
            int readBytes = 0;
            List<CompletedPart> completedParts = new ArrayList<>();
            while (((readBytes = is.read(byteArr))) != -1)
            {
                UploadPartRequest uploadPartRequest = UploadPartRequest.builder()
                    .bucket(bucket)
                    .key(artifactKey)
                    .uploadId(uploadId)
                    .partNumber(part)
                    .build();
                String etag = s3Client.uploadPart(uploadPartRequest, RequestBody.fromBytes(byteArr)).eTag();
                CompletedPart completedPart = CompletedPart.builder()
                    .partNumber(part)
                    .eTag(etag)
                    .build();
                completedParts.add(completedPart);
                part += 1;
            }

            CompletedMultipartUpload completedMultipartUpload = CompletedMultipartUpload.builder()
                .parts(completedParts)
                .build();

            CompleteMultipartUploadRequest completeMultipartUploadRequest =
                CompleteMultipartUploadRequest.builder()
                    .bucket(bucket)
                    .key(artifactKey)
                    .uploadId(uploadId)
                    .multipartUpload(completedMultipartUpload)
                    .build();

            s3Client.completeMultipartUpload(completeMultipartUploadRequest);
        }

        private void objectUpload(Path localArtifactPath, String artifactKey)
        {
            PutObjectRequest objectRequest = PutObjectRequest.builder()
                .bucket(bucket)
                .key(artifactKey)
                .build();
            s3Client.putObject(objectRequest, RequestBody.fromFile(localArtifactPath));
        }
    }
}
