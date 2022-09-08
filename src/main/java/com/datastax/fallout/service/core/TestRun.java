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
package com.datastax.fallout.service.core;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.yaml.snakeyaml.DumperOptions;
import org.yaml.snakeyaml.Yaml;

import com.datastax.driver.mapping.annotations.ClusteringColumn;
import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.PartitionKey;
import com.datastax.driver.mapping.annotations.Table;
import com.datastax.fallout.cassandra.shaded.com.google.common.annotations.VisibleForTesting;
import com.datastax.fallout.harness.TestDefinition;
import com.datastax.fallout.ops.ResourceRequirement;

import static com.datastax.fallout.runner.Artifacts.maybeStripGzSuffix;
import static com.datastax.fallout.util.YamlUtils.dumpYaml;
import static com.datastax.fallout.util.YamlUtils.loadYaml;

/** WARNING when making changes to this class be aware that both the QUEUE ({@link
 * com.datastax.fallout.runner.QueuingTestRunner}) and RUNNER ({@link com.datastax.fallout.runner.DirectTestRunner})
 * processes write this class to the DB via {@link com.datastax.fallout.service.db.TestRunDAO}.
 * Since a newer version of the QUEUE can be running at the same time as any number of older RUNNER
 * processes, it must be EITHER possible for older versions of this layout to be serialized to the
 * current DB schema OR a full shutdown of all RUNNER processes must be performed before redeploying. */
@Table(name = "test_runs")
public class TestRun implements ReadOnlyTestRun
{
    /** Also used as part of {@link com.datastax.fallout.runner.TestRunStatusUpdate} to communicate between
     *  processes; check the documentation for that class for what to bear in mind when making changes */
    public enum State
    {
        /** Initial post-creation state */
        CREATED,

        // Running test states:

        /** Test run is being converted to a runnable format from definition */
        PREPARING_RUN,

        /** Test run checking for availability of resources */
        CHECKING_RESOURCES,

        /** Resources are being reserved for the ensemble; after this state, we can safely assume that resources have been
         *  exclusively reserved for the ensemble.  Reservation is distinct from acquisition: for example, we
         *  can reserve some virtual server instances from a pool, but they need not have been created. */
        RESERVING_RESOURCES,

        /** Resources are being acquired */
        ACQUIRING_RESOURCES,

        /** All resources have been acquired, and the ensemble is now being setup for the workload */
        SETTING_UP,

        /** The test run is running: this means that we are actually executing the workload of the test. */
        RUNNING,

        /** The ensemble is being torn down */
        TEARING_DOWN,

        CHECKING_ARTIFACTS,

        // Terminal states

        PASSED,
        FAILED,
        /** test run failed due to external state that may change: we should try again later */
        WAITING_FOR_RESOURCES,
        /** test run was aborted */
        ABORTED,
        /** we lost track of this test run's state */
        UNKNOWN;

        public boolean waiting()
        {
            return this == CREATED || this == WAITING_FOR_RESOURCES;
        }

        public boolean active()
        {
            return !waiting() && !finished();
        }

        public boolean finished()
        {
            return this == FAILED || this == ABORTED || this == PASSED;
        }

        public boolean failed()
        {
            return this == FAILED || this == ABORTED || this == WAITING_FOR_RESOURCES;
        }

        public boolean resourcesChecked()
        {
            return active() && this.ordinal() > CHECKING_RESOURCES.ordinal();
        }

        public boolean resourcesReserved()
        {
            return active() && this.ordinal() > RESERVING_RESOURCES.ordinal();
        }
    }

    @PartitionKey(0)
    private String owner;

    @PartitionKey(1)
    private String testName;

    @ClusteringColumn
    private UUID testRunId;

    @Column
    private Date createdAt;

    @Column
    private Date startedAt;

    @Column
    private Date finishedAt;

    @Column
    private State state = State.CREATED;

    @Column
    private State failedDuring;

    @Column
    private String definition;

    @Column
    private String results;

    @Column
    private Map<String, Long> artifacts = new HashMap<>();

    /** Only needed for active test runs to make sure we don't hit the file system constantly.
     *  In particular, it isn't stored in DeletedTestRun nor does it take part in equals/hashCode */
    @Column
    private Date artifactsLastUpdated;

    @Column
    private String parsedLogInfo;

    @Column
    private String templateParams;

    @Column
    private TestCompletionNotification emailPref;

    @Column
    private TestCompletionNotification slackPref;

    @Column
    private Set<ResourceRequirement> resourceRequirements = Set.of();

    @Column
    private Map<String, String> links = new HashMap<>();

    @Column
    private boolean keepForever;

    public TestRun()
    {
    }

    TestRun(String owner, String testName, String definition, Map<String, Object> templateParams)
    {
        this(owner, testName, definition);
        setTemplateParamsMap(templateParams);
    }

    private TestRun(String owner, String testName, String definition)
    {
        this.owner = owner;
        this.testName = testName;
        this.testRunId = UUID.randomUUID();
        this.definition = definition;
        this.artifacts = new HashMap<>();
        this.keepForever = false;
    }

    /** Make a minimal copy suitable with none of the in-progress state: equivalent to calling
     * {@link Test#createTestRun} and then calculating the {@link #resourceRequirements}.  In particular,
     * the new copy will have a different {@link #testRunId}. */
    public TestRun copyForReRun()
    {
        final TestRun testRun = new TestRun(owner, testName, definition);
        testRun.templateParams = templateParams;
        testRun.resourceRequirements = resourceRequirements;
        return testRun;
    }

    /** Make a full copy, suitable for taking a snapshot of an active test run.
     *  FIXME: This is a perfect target for immutables.org's copy generation */
    public ReadOnlyTestRun immutableCopy()
    {
        TestRun testRun = new TestRun();

        testRun.owner = owner;
        testRun.testName = testName;
        testRun.testRunId = testRunId;
        testRun.createdAt = createdAt;
        testRun.startedAt = startedAt;
        testRun.finishedAt = finishedAt;
        testRun.state = state;
        testRun.failedDuring = failedDuring;
        testRun.definition = definition;
        testRun.results = results;
        testRun.artifacts = Map.copyOf(artifacts);
        testRun.artifactsLastUpdated = artifactsLastUpdated;
        testRun.parsedLogInfo = parsedLogInfo;
        testRun.templateParams = templateParams;
        testRun.emailPref = emailPref;
        testRun.slackPref = slackPref;
        // ResourceRequirements are sort-of immutable, so we can get away with not copying those.
        testRun.resourceRequirements = Set.copyOf(resourceRequirements);

        return testRun;
    }

    @Override
    public String getOwner()
    {
        return owner;
    }

    public void setOwner(String owner)
    {
        this.owner = owner;
    }

    @Override
    public String getTestName()
    {
        return testName;
    }

    public void setTestName(String testName)
    {
        this.testName = testName;
    }

    @Override
    public UUID getTestRunId()
    {
        return testRunId;
    }

    public void setTestRunId(UUID testRunId)
    {
        this.testRunId = testRunId;
    }

    @Override
    public Date getStartedAt()
    {
        return startedAt;
    }

    public void setStartedAt(Date startedAt)
    {
        this.startedAt = startedAt;
    }

    @Override
    public Date getCreatedAt()
    {
        // This handles compatibility with existing test runs at the expense of giving behaviour to a POD getter.
        return createdAt == null ? startedAt : createdAt;
    }

    public void setCreatedAt(Date createdAt)
    {
        this.createdAt = createdAt;
    }

    @Override
    public Date getFinishedAt()
    {
        return finishedAt;
    }

    public void setFinishedAt(Date finishedAt)
    {
        this.finishedAt = finishedAt;
    }

    @Override
    public String getDefinition()
    {
        return definition;
    }

    public void setDefinition(String yaml)
    {
        this.definition = yaml;
    }

    @JsonIgnore
    public String getExpandedDefinition()
    {
        Preconditions.checkState(getDefinition() != null);
        return TestDefinition.expandTemplate(getDefinition(), getTemplateParamsMap());
    }

    @Override
    public String getResults()
    {
        return results;
    }

    public void setResults(String results)
    {
        this.results = results;
    }

    @Override
    public State getState()
    {
        return state;
    }

    public void setState(State state)
    {
        this.state = state;
    }

    @Override
    public State getFailedDuring()
    {
        return failedDuring;
    }

    public void setFailedDuring(State state)
    {
        failedDuring = state;
    }

    @Override
    public Optional<Long> getArtifactsSizeBytes()
    {
        return artifacts.isEmpty() ? Optional.empty() :
            Optional.of(artifacts.values().stream().mapToLong(Long::longValue).sum());
    }

    @JsonIgnore
    public String getSizeOnDisk()
    {
        return getArtifactsSizeBytes().map(FileUtils::byteCountToDisplaySize).orElse("Unknown");
    }

    public Map<String, Long> getArtifacts()
    {
        return artifacts;
    }

    public Date getArtifactsLastUpdated()
    {
        return artifactsLastUpdated;
    }

    /**
     * Artifacts must be updated when:
     * 1. Fallout has no information about the artifacts in the database; OR
     * 2. The test run is active and has not been updated in the last 30 seconds.
     *
     * These two conditions exclude any situation where the artifacts are stored within the artifacts archive,
     * because artifacts are only archived after a test run has finished and been compressed. Updating artifacts
     * requires walking the filesystem, which is not supported by the
     * {@link com.datastax.fallout.service.artifacts.ArtifactArchive}
     */
    private boolean artifactsNeedUpdating()
    {
        Date thirtySecondsAgo = Date.from(Instant.now().minusSeconds(30));
        return this.artifacts.isEmpty() ||
            (this.state.active() &&
                (this.artifactsLastUpdated == null || this.artifactsLastUpdated.before(thirtySecondsAgo)));
    }

    public boolean updateArtifactsIfNeeded(Supplier<Map<String, Long>> artifactsSupplier)
    {
        final boolean updateNeeded = artifactsNeedUpdating();
        if (updateNeeded)
        {
            updateArtifacts(artifactsSupplier.get());
        }
        return updateNeeded;
    }

    /** Update the artifact list and return the overall size */
    public Long updateArtifacts(Map<String, Long> artifacts)
    {
        this.artifacts = artifacts.entrySet().stream()
            .collect(Collectors.toMap(e -> {
                // Handle files which have both a gzipped and un-gzipped version
                final String unstripped = e.getKey();
                final String stripped = maybeStripGzSuffix(unstripped);
                return artifacts.containsKey(stripped) ?
                    unstripped : stripped;
            }, Map.Entry::getValue));
        this.artifactsLastUpdated = Date.from(Instant.now());
        return this.artifacts.values().stream().mapToLong(Long::longValue).sum();
    }

    public String getParsedLogInfo()
    {
        return parsedLogInfo;
    }

    public void setParsedLogInfo(String parsedLogInfo)
    {
        this.parsedLogInfo = parsedLogInfo;
    }

    /** Mark a testrun as having started. */
    public void start()
    {
        setResults(null);
        setFailedDuring(null);
        setStartedAt(new Date());
        setFinishedAt(null);
    }

    @JsonProperty("templateParams")
    public String getTemplateParamsForJson()
    {
        return templateParams == null ? "{}" : templateParams;
    }

    @JsonProperty("templateParams")
    public void setTemplateParamsForJson(String params)
    {
        if (params.equals("{}"))
        {
            templateParams = null;
        }
        else
        {
            templateParams = params;
        }
    }

    public String getTemplateParams()
    {
        return templateParams;
    }

    public void setEmailPref(TestCompletionNotification emailPref)
    {
        this.emailPref = emailPref;
    }

    public TestCompletionNotification getEmailPref()
    {
        return emailPref;
    }

    public void setSlackPref(TestCompletionNotification slackPref)
    {
        this.slackPref = slackPref;
    }

    public TestCompletionNotification getSlackPref()
    {
        return slackPref;
    }

    @Override
    public Set<ResourceRequirement> getResourceRequirements()
    {
        return resourceRequirements;
    }

    public void setResourceRequirements(Set<ResourceRequirement> resourceRequirements)
    {
        this.resourceRequirements = resourceRequirements;
    }

    public static List<ResourceRequirement> getResourceRequirementsForTestRuns(List<? extends ReadOnlyTestRun> testRuns)
    {
        List<ResourceRequirement> reducedRequirements = new ArrayList<>(
            ResourceRequirement.reducedResourceRequirements(testRuns.stream()
                .map(ReadOnlyTestRun::getResourceRequirements)
                .filter(Objects::nonNull)
                .flatMap(Collection::stream)));

        reducedRequirements.sort(Comparator.comparing(ResourceRequirement::getResourceType));
        return reducedRequirements;
    }

    public Map<String, String> getLinks()
    {
        return links;
    }

    public void addLink(String linkname, String link)
    {
        links.put(linkname, link);
    }

    public void removeLinks()
    {
        links.clear();
    }

    @Override
    @JsonIgnore
    public Map<String, Object> getTemplateParamsMap()
    {
        return templateParams != null ?
            loadYaml(templateParams) :
            Map.of();
    }

    @JsonIgnore
    public void setTemplateParamsMap(final Map<String, Object> templateParams)
    {
        // Don't use ImmutableMap, as that forbids null entries, and those are valid for templateParams
        final Map<String, Object> paramsWithDefaults = new LinkedHashMap<>(
            definition != null ?
                TestDefinition.loadDefaults(TestDefinition.splitDefaultsAndDefinition(definition).getLeft()) :
                Map.of());

        paramsWithDefaults.putAll(templateParams);

        this.templateParams = paramsWithDefaults.isEmpty() ? null : dumpYaml(paramsWithDefaults);
    }

    @JsonIgnore
    public String getDefinitionWithTemplateParams()
    {
        if (getTemplateParamsMap().isEmpty())
        {
            return getDefinition();
        }

        DumperOptions options = new DumperOptions();
        options.setPrettyFlow(true);
        options.setDefaultFlowStyle(DumperOptions.FlowStyle.BLOCK);
        String prettyTemplateParams = new Yaml(options).dump(getTemplateParamsMap());

        Pair<Optional<String>, String> defaultsAndDefinition =
            TestDefinition.splitDefaultsAndDefinition(getDefinition());

        String defaults = defaultsAndDefinition.getLeft()
            .map(defaults_ -> "# DEFAULT PARAMETERS:\n" +
                defaults_.replaceAll("(\\A|\\n)", "$1# ") +
                "\n")
            .orElse("");
        String definition = defaultsAndDefinition.getRight();

        return String.format(
            "# PARAMETERS FOR TEST RUN %s, %s, %s:\n",
            getOwner(),
            getTestName(),
            getTestRunId()) +
            prettyTemplateParams + "\n" +
            defaults +
            "---\n" +
            definition;
    }

    public boolean keepForever()
    {
        return keepForever;
    }

    @VisibleForTesting
    public void setKeepForever(boolean keepForever)
    {
        this.keepForever = keepForever;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        TestRun testRun = (TestRun) o;

        if (owner != null ? !owner.equals(testRun.owner) : testRun.owner != null)
            return false;
        if (emailPref != null ? !emailPref.equals(testRun.emailPref) : testRun.emailPref != null)
            return false;
        if (slackPref != null ? !slackPref.equals(testRun.slackPref) : testRun.slackPref != null)
            return false;
        if (testName != null ? !testName.equals(testRun.testName) : testRun.testName != null)
            return false;
        if (testRunId != null ? !testRunId.equals(testRun.testRunId) : testRun.testRunId != null)
            return false;
        if (createdAt != null ? !createdAt.equals(testRun.createdAt) : testRun.createdAt != null)
            return false;
        if (startedAt != null ? !startedAt.equals(testRun.startedAt) : testRun.startedAt != null)
            return false;
        if (finishedAt != null ? !finishedAt.equals(testRun.finishedAt) : testRun.finishedAt != null)
            return false;
        if (state != testRun.state)
            return false;
        if (failedDuring != testRun.failedDuring)
            return false;
        if (definition != null ? !definition.equals(testRun.definition) : testRun.definition != null)
            return false;
        if (results != null ? !results.equals(testRun.results) : testRun.results != null)
            return false;
        if (artifacts != null ? !artifacts.equals(testRun.artifacts) : testRun.artifacts != null)
            return false;
        if (parsedLogInfo != null ? !parsedLogInfo.equals(testRun.parsedLogInfo) : testRun.parsedLogInfo != null)
            return false;
        if (resourceRequirements != null ? !resourceRequirements.equals(testRun.resourceRequirements) :
            testRun.resourceRequirements != null)
            return false;
        if (links != null ? !links.equals(testRun.links) : testRun.links != null)
            return false;
        return !(templateParams != null ? !templateParams.equals(testRun.templateParams) :
            testRun.templateParams != null);
    }

    @Override
    public int hashCode()
    {
        int result = owner != null ? owner.hashCode() : 0;
        result = 31 * result + (testName != null ? testName.hashCode() : 0);
        result = 31 * result + (emailPref != null ? emailPref.hashCode() : 0);
        result = 31 * result + (slackPref != null ? slackPref.hashCode() : 0);
        result = 31 * result + (testRunId != null ? testRunId.hashCode() : 0);
        result = 31 * result + (createdAt != null ? createdAt.hashCode() : 0);
        result = 31 * result + (startedAt != null ? startedAt.hashCode() : 0);
        result = 31 * result + (finishedAt != null ? finishedAt.hashCode() : 0);
        result = 31 * result + (state != null ? state.hashCode() : 0);
        result = 31 * result + (failedDuring != null ? failedDuring.hashCode() : 0);
        result = 31 * result + (definition != null ? definition.hashCode() : 0);
        result = 31 * result + (results != null ? results.hashCode() : 0);
        result = 31 * result + (artifacts != null ? artifacts.hashCode() : 0);
        result = 31 * result + (parsedLogInfo != null ? parsedLogInfo.hashCode() : 0);
        result = 31 * result + (templateParams != null ? templateParams.hashCode() : 0);
        result = 31 * result + (resourceRequirements != null ? resourceRequirements.hashCode() : 0);
        result = 31 * result + (links != null ? links.hashCode() : 0);
        return result;
    }

    @Override
    public String toString()
    {
        return "TestRun{" +
            "owner='" + owner + '\'' +
            ", name='" + testName + '\'' +
            ", params='" + templateParams + '\'' +
            ", emailPref='" + emailPref + '\'' +
            ", slackPref='" + slackPref + '\'' +
            ", testRunId=" + testRunId +
            ", createdAt=" + createdAt +
            ", startedAt=" + startedAt +
            ", finishedAt=" + finishedAt +
            ", state=" + state +
            ", failedDuring=" + failedDuring +
            ", definition='" + definition + '\'' +
            ", results='" + results + '\'' +
            ", artifacts=" + artifacts +
            ", artifactsLastUpdated=" + artifactsLastUpdated +
            ", parsedLogInfo=" + parsedLogInfo +
            ", resourceRequirements=" + resourceRequirements +
            ", links=" + links +
            '}';
    }
}
