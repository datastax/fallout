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

import java.util.Collections;
import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableMap;
import org.assertj.core.api.Assertions;
import org.assertj.core.api.ThrowableAssert;
import org.junit.Test;

import com.datastax.fallout.exceptions.InvalidConfigurationException;
import com.datastax.fallout.ops.EnsembleBuilder;
import com.datastax.fallout.ops.NodeGroup;
import com.datastax.fallout.ops.provisioner.kubernetes.GoogleKubernetesEngineProvisioner;

import static com.datastax.fallout.harness.TestResultAssert.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

public class ActiveTestRunBuilderTest extends EnsembleFalloutTest
{
    private void expectValidationError(String errorMessage, String yamlPath)
    {
        expectError(InvalidConfigurationException.class, errorMessage, () -> createTest(yamlPath));
    }

    private <T extends Throwable> void expectError(Class<T> expectedExceptionType, String errorMessage,
        ThrowableAssert.ThrowingCallable code)
    {
        assertThatExceptionOfType(expectedExceptionType)
            .isThrownBy(code)
            .withMessageContaining(errorMessage);
    }

    private void createTest(String yamlPath)
    {
        createTest(yamlPath, Collections.emptyMap());
    }

    private ActiveTestRun createTest(String yamlPath, Map<String, Object> templateParams)
    {
        String yamlContent = readYamlFile(yamlPath);
        return createTestWithContent(yamlContent, templateParams);
    }

    private ActiveTestRun createTestWithContent(String yamlContent)
    {
        return createTestWithContent(yamlContent, Collections.emptyMap());
    }

    private ActiveTestRun createTestWithContent(String yamlContent, Map<String, Object> templateParams)
    {
        String expandedYaml = TestDefinition.expandTemplate(yamlContent, templateParams);
        return createActiveTestRunBuilder()
            .withEnsembleFromYaml(expandedYaml)
            .withWorkloadFromYaml(expandedYaml)
            .build();
    }

    @Test
    public void only_uses_the_last_document_as_the_definition()
    {
        String yamlContent = "random_yaml_text: 42\n" +
            "---\n" +
            readYamlFile("fakes.yaml");
        createTestWithContent(yamlContent);
    }

    @Test
    public void expands_mustache_tags_with_defaults()
    {
        createTest("template-test-yamls/complete-defaults.yaml");
    }

    @Test
    public void fails_on_missing_tag_values()
    {
        expectValidationError("no_default_provided_for_this_tag",
            "template-test-yamls/partial-defaults.yaml");
    }

    @Test
    public void tag_values_can_be_overridden()
    {
        Map<String, Object> invalidParams = ImmutableMap.<String, Object>builder()
            .put("dummy_module", "fail")
            .build();
        ActiveTestRun activeTestRun = createTest("template-test-yamls/complete-defaults.yaml",
            invalidParams);
        TestResult testResult = performTestRun(activeTestRun);
        assertThat(testResult).isNotValid();
    }

    @Test
    public void fails_on_invalid_module_indent()
    {
        expectValidationError(
            "Found invalid yaml keys below module 'my_module': [module_with_wrong_indent]. Only valid keys are [properties, module]",
            "invalid_indent_module.yaml");
    }

    @Test
    public void fails_on_invalid_checker_indent()
    {
        expectValidationError(
            "Found invalid yaml keys below checker 'my_checker': [checker_with_wrong_indent]. Only valid keys are [properties, checker]",
            "invalid_indent_checker.yaml");
    }

    @Test
    public void fails_on_invalid_artifact_checker_indent()
    {
        expectValidationError(
            "Found invalid yaml keys below artifact_checker 'my_artifact_checker': [artifact_checker_with_wrong_indent]. Only valid keys are [artifact_checker, properties]",
            "invalid_indent_artifact_checker.yaml");
    }

    @Test
    public void fails_on_duplicate_checkers()
    {
        expectValidationError("Duplicate Checker name",
            "duplicate_checkers_fail.yaml");
    }

    @Test
    public void fails_on_duplicate_yaml_keys()
    {
        expectValidationError("duplicate key checker1",
            "duplicate_keys_fail.yaml");
    }

    @Test
    public void fails_on_duplicate_module()
    {
        expectValidationError("Duplicate module or subphase aliases: auto_fail",
            "badalias.yaml");
    }

    @Test
    public void fail_on_invalid_toplevel_yaml_key()
    {
        expectValidationError(
            "Found invalid yaml keys at the top level: [invalid_top_level_key_1, invalid_top_level_key_2]. Only valid keys are [ensemble, workload]",
            "invalid_toplevel_key.yaml");
    }

    @Test
    public void fail_on_invalid_toplevel_workload_yaml_key()
    {
        expectValidationError(
            "Found invalid yaml keys below workload: [invalid_workload_key_1, invalid_workload_key_2]. Only valid keys are [phases, checkers, artifact_checkers]",
            "invalid_workload_key.yaml");
    }

    @Test
    public void fail_on_invalid_provisioner_key()
    {
        expectValidationError("Found invalid yaml keys below provisioner of nodegroup server: [my_invalid_key]",
            "invalid_provisioner_key.yaml");
    }

    @Test
    public void fail_on_invalid_configuration_manager_key()
    {
        expectValidationError(
            "Found invalid yaml keys below configuration_manager of nodegroup server: [my_invalid_key]",
            "invalid_cfgmgr_key.yaml");
    }

    @Test
    public void fail_on_invalid_nodegroup_key()
    {
        expectValidationError("Found invalid yaml keys below nodegroup server: [my_invalid_key]",
            "invalid_nodegroup_key.yaml");
    }

    @Test
    public void fail_on_invalid_artifact_checker_properties()
    {
        expectValidationError("Unknown properties detected: [fallout.artifact_checkers.hdrhistogram.bogus]",
            "invalid_artifact_checker_property.yaml");
    }

    @Test
    public void fail_on_invalid_checker_properties()
    {
        expectValidationError("Unknown properties detected: [fallout.checkers.nofail.bogus]",
            "invalid_checker_property.yaml");
    }

    @Test
    public void fail_on_invalid_module_properties()
    {
        expectValidationError("Unknown properties detected: [test.module.fail.bogus]",
            "invalid_module_property.yaml");
    }

    @Test
    public void fail_on_unsatisfiable_configuration_manager_dependencies()
    {
        expectValidationError(
            "Required: [class com.datastax.fallout.ops.providers.KubeControlProvider, class com.datastax.fallout.ops.providers.DataStaxCassOperatorProvider]",
            "missing-dependency.yaml");
    }

    @Test
    public void fail_on_malformed_yaml_url()
    {
        expectValidationError("The provided yaml_url is not a valid URL.",
            "malformed_yaml_url.yaml");
    }

    @Test
    public void fail_on_invalid_yaml_at_url()
    {
        expectValidationError("Could not read a valid test yaml from the provided yaml_url.",
            "invalid_yaml_at_url.yaml");
    }

    @Test
    public void fail_on_yaml_url_with_local_defaults()
    {
        expectValidationError(
            "Local defaults are not allowed when importing from a remote yaml. Found the following defaults: Optional[foo: foo]",
            "local_defaults_with_yaml_url.yaml");
    }

    @Test
    public void fail_if_missing_ensemble()
    {
        expectError(IllegalArgumentException.class, "Ensemble is missing", () -> createActiveTestRunBuilder()
            .withWorkloadFromYaml("fakes.yaml")
            .build());
    }

    @Test
    public void fail_if_missing_workload()
    {
        expectError(IllegalArgumentException.class, "Workload is missing", () -> createActiveTestRunBuilder()
            .withEnsembleBuilder(EnsembleBuilder.create(), true)
            .build());
    }

    @Test
    public void fail_on_duplicate_explicit_cluster_names()
    {
        expectError(IllegalArgumentException.class, "Duplicate node group name: my_duplicate_cluster",
            () -> createTest("invalid_duplicate_cluster_names.yaml"));
    }

    @Test
    public void fail_if_nodegroup_mark_for_reuse_and_runlevel_final_properties_are_both_set()
    {
        expectValidationError("Cannot set mark_for_reuse and runlevel.final at the same time.",
            "nodegroup_mark_for_reuse_with_runlevel_final.yaml");
    }

    @Test
    public void fail_if_gke_master_cluster_version_is_invalid()
    {
        expectValidationError("Invalid cluster version: this_is_not_a_version Valid versions:", "gke_version.yaml");
    }

    @Test
    public void gke_cluster_versions_can_be_fetched_and_are_valid()
    {
        NodeGroup server = createTest("gke_version.yaml", ImmutableMap.of("set_cluster_version", false)).getEnsemble()
            .getServerGroup("server");
        List<String> validGKEClusterVersions = ((GoogleKubernetesEngineProvisioner) server.getProvisioner())
            .getValidGKEClusterVersions();
        Assertions.assertThat(validGKEClusterVersions).isNotEmpty();
        createTest("gke_version.yaml",
            ImmutableMap.of("gke_cluster_version", validGKEClusterVersions.get(0)));
    }

    @Test
    public void fail_if_file_spec_is_given_more_than_one_type()
    {
        expectValidationError(
            "Incorrect file spec type. There should be exactly one file spec type per item in the files list.",
            "multiple_file_spec_types.yaml");
    }

    @Test
    public void fail_if_incorrect_data_structure_for_file_spec_type_is_used()
    {
        expectValidationError(
            "Url file 'file.txt' expects value to be a valid url, not 'this is not a valid url'",
            "incorrect_file_spec_type.yaml");
    }

    @Test
    public void fail_if_file_spec_paths_are_duplicated()
    {
        expectValidationError("Some file spec paths are not unique:", "duplicate_file_spec_paths.yaml");
    }

    @Test
    public void fail_if_file_spec_is_declared_with_unknown_type()
    {
        expectValidationError("File 'foo' has an unknown file type 'bar'", "unknown_file_spec_type.yaml");
    }

    @Test
    public void fail_if_kubernetes_manifest_is_not_a_managed_file()
    {
        expectValidationError("Value must be a managed file, but found not_a_managed_file",
            "unmanaged_kubernetes_manifest.yaml");
    }

    @Test
    public void fail_if_both_file_pattern_and_file_path_are_set_in_regex_artifact_checker()
    {
        expectValidationError("Exactly one of file_path or file_regex must be set",
            "multiple_artifact_descriptors_set.yaml");
    }
}
