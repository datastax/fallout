package com.datastax.fallout.ops.configmanagement;

import org.assertj.core.api.ThrowableAssert;
import org.junit.Test;

import com.datastax.fallout.test.utils.WithTestResources;

import static com.datastax.fallout.ops.configmanagement.kubernetes.DataStaxCassOperatorConfigurationManager.getClusterService;
import static com.datastax.fallout.ops.configmanagement.kubernetes.DataStaxCassOperatorConfigurationManager.getOperatorImage;
import static com.datastax.fallout.ops.configmanagement.kubernetes.DataStaxCassOperatorConfigurationManager.getServerImage;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

public class DataStaxCassOperatorParsingTest extends WithTestResources
{
    @Test
    public void server_image_can_be_parsed_from_datacenter_crd()
    {
        assertThat(getServerImage(getTestClassResource("ds-cass-datacenter.yaml")))
            .isEqualTo("datastax/cassandra-mgmtapi-3_11_6:v0.1.0");
    }

    @Test
    public void datacenter_crd_missing_server_images_throws_exception_with_good_message()
    {
        assertRuntimeExceptionWithMessage(
            () -> getServerImage(getTestClassResource("ds-cass-datacenter-without-image.yaml")),
            "serverImage is required to be present in the CassandraDatacenter definition");
    }

    @Test
    public void operator_image_can_be_parsed_from_operator_deployment()
    {
        assertThat(getOperatorImage(getTestClassResource("ds-cass-operator.yaml")))
            .isEqualTo("datastax/cass-operator:1.1.0");
    }

    @Test
    public void operator_deployment_missing_image_throws_exception_with_good_message()
    {
        assertRuntimeExceptionWithMessage(
            () -> getOperatorImage(getTestClassResource("ds-cass-operator-without-image.yaml")),
            "Could not get operator image from manifest");
    }

    @Test
    public void cluster_service_can_be_parsed_from_datacenter_crd()
    {
        assertThat(getClusterService(getTestClassResource("ds-cass-datacenter.yaml")))
            .isEqualTo("cluster1-dc1-service");
    }

    @Test
    public void datacenter_crd_where_cluster_service_cannot_be_parsed_throws_exception_with_good_message()
    {
        assertRuntimeExceptionWithMessage(
            () -> getClusterService(getTestClassResource("ds-cass-datacenter-without-name.yaml")),
            "clusterName and datacenter name are required in the CassandraDatacenter definition");
    }

    private void assertRuntimeExceptionWithMessage(ThrowableAssert.ThrowingCallable code, String message)
    {
        assertThatExceptionOfType(RuntimeException.class)
            .isThrownBy(code)
            .withMessage(message);
    }
}
