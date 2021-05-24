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
package com.datastax.fallout.runner;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.quicktheories.api.TriConsumer;
import org.quicktheories.core.Gen;

import com.datastax.fallout.ops.ResourceRequirement;
import com.datastax.fallout.ops.ResourceType;
import com.datastax.fallout.service.core.Fakes;
import com.datastax.fallout.service.core.Test;

import static com.datastax.fallout.ops.ResourceRequirementHelpers.req;
import static com.datastax.fallout.runner.ResourceLimitHelpers.limit;
import static com.datastax.fallout.test.utils.QtHelpers.named;
import static org.assertj.core.api.BDDAssertions.then;
import static org.quicktheories.QuickTheory.qt;
import static org.quicktheories.generators.SourceDSL.arbitrary;
import static org.quicktheories.generators.SourceDSL.arrays;
import static org.quicktheories.generators.SourceDSL.integers;

public class ResourceLimiterTest
{
    private List<ResourceLimit> limits = List.of();
    private List<ResourceRequirement> inUse = List.of();
    private Set<ResourceRequirement> required = Set.of();

    class Given
    {
        private Given limits(ResourceLimit... limits_)
        {
            limits = List.of(limits_);
            return this;
        }

        private Given inUse(ResourceRequirement... inUse_)
        {
            inUse = List.of(inUse_);
            return this;
        }

        private Given required(ResourceRequirement... required_)
        {
            // We use "stream(required_).collect(toSet)" instead of "Set.of(required_)"
            // because the former handles duplicates in required_ and the latter does not.

            required = Arrays.stream(required_).collect(Collectors.toSet());
            return this;
        }
    }

    private Given given()
    {
        return new Given();
    }

    private void thenResourcesAreAvailableIf(boolean condition)
    {
        var checker = new ResourceLimiter(() -> inUse, limits);

        var testRunFactory = new Fakes.TestRunFactory();
        var test = Test.createTest(Fakes.TEST_NAME, Fakes.TEST_NAME, "");
        var testRun = testRunFactory.makeTestRun(test);
        testRun.setResourceRequirements(required);

        var available = checker.test(testRun);
        then(available).isEqualTo(condition);
    }

    private void thenResourcesAreAvailable()
    {
        thenResourcesAreAvailableIf(true);
    }

    private Gen<ResourceType> resourceTypes()
    {
        var providers = arbitrary().pick("openstack", "nebula", "ec2");
        var tenants = arbitrary().pick("performance", "marketing", "development");
        var instanceTypes = arbitrary().pick("i4.xlarge", "ms1.small", "m3.large");

        return providers.zip(tenants, instanceTypes,
            (provider, tenant, instanceType) -> new ResourceType(provider, tenant, instanceType, Optional.empty()));
    }

    private Gen<ResourceRequirement> resourceRequirements()
    {
        return resourceTypes().zip(integers().all(), ResourceRequirement::new);
    }

    private Gen<ResourceRequirement[]> resourceRequirementArrays()
    {
        return arrays().ofClass(resourceRequirements(), ResourceRequirement.class).withLengthBetween(0, 5);
    }

    @org.junit.jupiter.api.Test
    public void when_no_limits_are_specified_then_resources_are_always_available()
    {
        qt()
            .forAll(resourceRequirementArrays(), resourceRequirementArrays())
            .checkAssert((inUse, required) -> {

                given()
                    .limits()
                    .inUse(inUse)
                    .required(required);

                thenResourcesAreAvailable();
            });
    }

    @org.junit.jupiter.api.Test
    public void non_matching_requirements_are_always_available()
    {
        qt()
            .forAll(resourceRequirementArrays(), resourceRequirementArrays())
            .checkAssert((inUse, required) -> {

                given()
                    .limits(limit("bogus", "bogus", "bogus", 0))
                    .inUse(inUse)
                    .required(required);

                thenResourcesAreAvailable();
            });
    }

    private static class Matches
    {
        final int matching;
        final int nonMatching;

        private Matches(int matching, int nonMatching)
        {
            this.matching = matching;
            this.nonMatching = nonMatching;
        }

        @Override
        public String toString()
        {
            return String.format("{%d, %d}", matching, nonMatching);
        }
    }

    private Gen<Integer> nodeCounts()
    {
        return integers().between(1, 10);
    }

    private Gen<Matches> matches()
    {
        return nodeCounts().zip(nodeCounts(), Matches::new);
    }

    @org.junit.jupiter.api.Test
    public void matching_limits_when_nothing_in_use_are_limited()
    {
        qt()
            .forAll(
                named("limit", nodeCounts()),
                named("required", nodeCounts()))
            .checkAssert((limit, required) -> {

                given()
                    .limits(limit("openstack", "performance", "ms1.small", limit))
                    .inUse()
                    .required(req("openstack", "performance", "ms1.small", required));

                thenResourcesAreAvailableIf(required <= limit);
            });
    }

    private void checkResourcesAreAvailableIfLimitsAreSatisfied(TriConsumer<Matches, Matches[], Matches> given)
    {
        qt()
            .forAll(
                named("limit", matches()),
                named("inUse", arrays().ofClass(matches(), Matches.class).withLength(3)),
                named("required", matches()))
            .checkAssert((limit, inUse, required) -> {
                given.accept(limit, inUse, required);

                thenResourcesAreAvailableIf(
                    required.matching + inUse[0].matching + inUse[1].matching <= limit.matching);
            });
    }

    @org.junit.jupiter.api.Test
    public void matching_works_when_provider_and_tenant_and_instance_type_specified()
    {
        final var provider = "openstack";
        final var tenant = "performance";
        final var matchingInstanceType = "ms1.small";
        final var nonMatchingLimitedInstanceType = "ms2.small";
        final var nonMatchingRequiredInstanceType = "c3.xlarge";

        checkResourcesAreAvailableIfLimitsAreSatisfied((limit, inUse, required) -> {
            given()
                .limits(
                    limit(provider, tenant, matchingInstanceType, limit.matching),
                    limit(provider, tenant, nonMatchingLimitedInstanceType, limit.nonMatching))
                .inUse(
                    req(provider, tenant, matchingInstanceType, inUse[0].matching),
                    req(provider, tenant, matchingInstanceType, inUse[1].matching),
                    req(provider, tenant, nonMatchingRequiredInstanceType, inUse[2].nonMatching))
                .required(
                    req(provider, tenant, matchingInstanceType, required.matching),
                    req(provider, tenant, nonMatchingRequiredInstanceType, required.nonMatching));
        });
    }

    @org.junit.jupiter.api.Test
    public void matching_works_when_only_provider_and_tenant_specified()
    {
        final var provider = "openstack";
        final var matchingTenant = "performance";
        final var nonMatchingLimitedTenant = "sales";
        final var nonMatchingRequiredTenant = "development";
        final var instanceType1 = "ms1.small";
        final var instanceType2 = "ms2.small";

        checkResourcesAreAvailableIfLimitsAreSatisfied((limit, inUse, required) -> {
            given()
                .limits(
                    limit(provider, matchingTenant, limit.matching),
                    limit(provider, nonMatchingLimitedTenant, limit.nonMatching))
                .inUse(
                    req(provider, matchingTenant, instanceType1, inUse[0].matching),
                    req(provider, matchingTenant, instanceType2, inUse[1].matching),
                    req(provider, nonMatchingRequiredTenant, instanceType1, inUse[2].nonMatching))
                .required(
                    req(provider, matchingTenant, instanceType1, required.matching),
                    req(provider, nonMatchingRequiredTenant, instanceType1, required.nonMatching));
        });
    }

    static final String matchingProvider = "openstack";
    static final String nonMatchingLimitedProvider = "nebula";
    static final String nonMatchingRequiredProvider = "ec2";
    static final String tenant1 = "performance";
    static final String tenant2 = "development";
    static final String instanceType = "ms1.small";

    @org.junit.jupiter.api.Test
    public void matching_works_when_only_provider_specified()
    {
        checkResourcesAreAvailableIfLimitsAreSatisfied((limit, inUse, required) -> {
            given()
                .limits(
                    limit(matchingProvider, limit.matching),
                    limit(nonMatchingLimitedProvider, limit.nonMatching))
                .inUse(
                    req(matchingProvider, tenant1, instanceType, inUse[0].matching),
                    req(matchingProvider, tenant2, instanceType, inUse[1].matching),
                    req(nonMatchingRequiredProvider, tenant1, instanceType, inUse[2].nonMatching))
                .required(
                    req(matchingProvider, tenant1, instanceType, required.matching),
                    req(nonMatchingRequiredProvider, tenant1, instanceType, required.nonMatching));
        });
    }

    @org.junit.jupiter.api.Test
    public void matching_ignores_named_instance_types_in_requirements()
    {
        checkResourcesAreAvailableIfLimitsAreSatisfied((limit, inUse, required) -> {
            given()
                .limits(
                    limit(matchingProvider, limit.matching),
                    limit(nonMatchingLimitedProvider, limit.nonMatching))
                .inUse(
                    req(matchingProvider, tenant1, instanceType, inUse[0].matching),
                    req(matchingProvider, tenant2, instanceType, inUse[1].matching),
                    req(nonMatchingRequiredProvider, tenant1, instanceType, inUse[2].nonMatching))
                .required(
                    req(matchingProvider, tenant1, instanceType, required.matching),
                    req(matchingProvider, tenant1, instanceType, "named", required.nonMatching));
        });
    }

    @org.junit.jupiter.api.Test
    public void matching_heeds_named_instance_types_that_are_in_use()
    {
        checkResourcesAreAvailableIfLimitsAreSatisfied((limit, inUse, required) -> {
            given()
                .limits(
                    limit(matchingProvider, limit.matching),
                    limit(nonMatchingLimitedProvider, limit.nonMatching))
                .inUse(
                    req(matchingProvider, tenant1, instanceType, "named", inUse[0].matching),
                    req(matchingProvider, tenant2, instanceType, inUse[1].matching),
                    req(nonMatchingRequiredProvider, tenant1, instanceType, inUse[2].nonMatching))
                .required(
                    req(matchingProvider, tenant1, instanceType, required.matching),
                    req(nonMatchingRequiredProvider, tenant1, instanceType, required.nonMatching));
        });
    }
}
