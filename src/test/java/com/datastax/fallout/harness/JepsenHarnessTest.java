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
package com.datastax.fallout.harness;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.fallout.harness.artifact_checkers.SystemLogChecker;
import com.datastax.fallout.harness.impl.LiveCountModule;
import com.datastax.fallout.harness.impl.MaxValueChecker;
import com.datastax.fallout.ops.ConfigurationManager;
import com.datastax.fallout.ops.EnsembleBuilder;
import com.datastax.fallout.ops.NodeGroupBuilder;
import com.datastax.fallout.ops.Provisioner;
import com.datastax.fallout.ops.WritablePropertyGroup;
import com.datastax.fallout.ops.impl.TestConfigurationManager;
import com.datastax.fallout.ops.provisioner.LocalProvisioner;

import static com.datastax.fallout.harness.JepsenApi.VALID;

public class JepsenHarnessTest extends EnsembleFalloutTest
{
    private final static Logger logger = LoggerFactory.getLogger(JepsenHarnessTest.class);

    @Test
    public void testFakes()
    {
        assertYamlFileValidates("fakes.yaml");
    }

    @Test
    public void testCheckerFailure()
    {
        TestResult result = runYamlFile("failtest.yaml");
        Boolean innerValid = (Boolean) ((Map) result.results()
            .get("verify_omitted_failures")).get(VALID);

        Assert.assertFalse(result.isValid());
        Assert.assertTrue(innerValid);
    }

    @Test
    public void testNoEmit()
    {
        maybeAssertYamlFileRunsAndFails("noemit.yaml");
    }

    @Test
    public void testInvalidRunlevels()
    {
        maybeAssertYamlFileRunsAndFails("invalid-runlevels.yaml");
    }

    @Test
    public void testValidRunlevels()
    {
        maybeAssertYamlFileRunsAndPasses("valid-runlevels.yaml");
    }

    @Test
    public void testSetupTeardownLifecycle()
    {
        // We just need simple provisioner/cm for the test lifecycle
        Provisioner provisioner = new LocalProvisioner();
        ConfigurationManager configurationManager = new TestConfigurationManager();

        WritablePropertyGroup properties = new WritablePropertyGroup();

        NodeGroupBuilder sgBuilder = NodeGroupBuilder
            .create()
            .withName("server")
            .withProvisioner(provisioner)
            .withConfigurationManager(configurationManager)
            .withPropertyGroup(properties)
            .withNodeCount(1);

        EnsembleBuilder ensembleBuilder = EnsembleBuilder.create()
            .withServerGroup(sgBuilder)
            .withClientGroup(sgBuilder);

        Map<String, Checker> checkers = new HashMap<>();
        Checker maxValue = new MaxValueChecker();

        Map<String, ArtifactChecker> artifactCheckers = new HashMap<>();
        ArtifactChecker syslog = new SystemLogChecker();
        artifactCheckers.put("syslog_check", syslog);

        // To start, we should only have one module that is set up without being torndown
        WritablePropertyGroup mvCheckerProperties = new WritablePropertyGroup();
        mvCheckerProperties.put(maxValue.prefix() + "value", "1");
        maxValue.setProperties(mvCheckerProperties);

        checkers.put("max_live_nodes", maxValue);

        List<Phase> phases = new ArrayList<>();
        Map<String, Module> firstPhase = new HashMap<>();
        Map<String, Module> secondPhase = new HashMap<>();
        Map<String, Module> thirdPhase = new HashMap<>();
        Module firstLCM = new LiveCountModule();
        firstLCM.setInstanceName("first_live_count");
        Module secondLCM = new LiveCountModule();
        secondLCM.setInstanceName("second_live_count");
        Module thirdLCM = new LiveCountModule();
        thirdLCM.setInstanceName("third_live_count");
        firstPhase.put("first_live_count", firstLCM);
        secondPhase.put("second_live_count", secondLCM);
        thirdPhase.put("third_live_count", thirdLCM);
        phases.add(new Phase("one", firstPhase));
        phases.add(new Phase("two", secondPhase));
        phases.add(new Phase("three", thirdPhase));

        ActiveTestRun activeTestRun = createActiveTestRunBuilder()
            .withEnsembleBuilder(ensembleBuilder, true)
            .withWorkload(new Workload(phases, checkers, artifactCheckers))
            .build();

        TestResult result = performTestRun(activeTestRun);
        Assert.assertTrue(result.isValid());

        // Change one module to global setup/teardown, so there should sometimes be two set up
        mvCheckerProperties.put(maxValue.prefix() + "value", "2");
        firstLCM.useGlobalSetupTeardown = true;

        result = performTestRun(activeTestRun);
        Assert.assertTrue(result.isValid());
    }
}
