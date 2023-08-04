/*
 * Copyright 2023 DataStax, Inc.
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
package com.datastax.fallout.components.kubernetes;

import java.util.Set;

import com.fasterxml.jackson.databind.JsonNode;
import org.junit.jupiter.api.Test;

import com.datastax.fallout.util.JsonUtils;

import static com.datastax.fallout.assertj.Assertions.assertThat;

public class JsonNodesTest
{

    private static final String jsonPayload =
        """
            {
               "apiVersion": "v1",
               "kind": "PersistentVolume",
               "metadata": {
                   "annotations": {
                       "pv.kubernetes.io/provisioned-by": "pd.csi.storage.gke.io",
                       "volume.kubernetes.io/provisioner-deletion-secret-name": "",
                       "volume.kubernetes.io/provisioner-deletion-secret-namespace": ""
                   },
                   "creationTimestamp": "2023-08-03T16:50:53Z",
                   "finalizers": [
                       "kubernetes.io/pv-protection",
                       "external-attacher/pd-csi-storage-gke-io"
                   ],
                   "name": "pvc-10f49bd8-d0bb-4b68-a0f6-6153b89c6fdb",
                   "resourceVersion": "6210",
                   "uid": "ab5f3de0-c9f4-4608-ab67-c7088dfc94e4"
               },
               "spec": {
                   "accessModes": [
                       "ReadWriteOnce"
                   ],
                   "capacity": {
                       "storage": "20Gi"
                   },
                   "claimRef": {
                       "apiVersion": "v1",
                       "kind": "PersistentVolumeClaim",
                       "name": "pulsar-bookkeeper-journal-pulsar-bookkeeper-0",
                       "namespace": "mypulsar",
                       "resourceVersion": "6160",
                       "uid": "10f49bd8-d0bb-4b68-a0f6-6153b89c6fdb"
                   },
                   "csi": {
                       "driver": "pd.csi.storage.gke.io",
                       "fsType": "ext4",
                       "volumeAttributes": {
                           "storage.kubernetes.io/csiProvisionerIdentity": "1691080879534-8081-pd.csi.storage.gke.io"
                       },
                       "volumeHandle": "projects/datastax-gcp-pulsar/zones/us-central1-a/disks/pvc-10f49bd8-d0bb-4b68-a0f6-6153b89c6fdb"
                   },
                   "nodeAffinity": {
                       "required": {
                           "nodeSelectorTerms": [
                               {
                                   "matchExpressions": [
                                       {
                                           "key": "topology.gke.io/zone",
                                           "operator": "In",
                                           "values": [
                                               "us-central1-a", "us-centra1-c"
                                           ]
                                       }
                                   ]
                               }
                           ]
                       }
                   },
                   "persistentVolumeReclaimPolicy": "Delete",
                   "storageClassName": "standard-rwo",
                   "volumeMode": "Filesystem"
               },
               "status": {
                   "phase": "Bound"
               }
            }

                   """;

    @Test
    void gcePersistentVolumes()
    {
        final GoogleKubernetesEngineProvisioner gkep = new GoogleKubernetesEngineProvisioner();
        final JsonNode item = JsonUtils.getJsonNode(jsonPayload);
        final String name = gkep.fetchNameFrom(item);
        assertThat(name).isNotEmpty();

        final String zonesLegacy = gkep.fetchZonesFromUsingLegacy(item);
        assertThat(zonesLegacy).isNull();

        final Set<String> multipleZones = gkep.fetchZonesFrom(item);
        assertThat(multipleZones).isNotEmpty();
        assertThat(multipleZones.size()).isEqualTo(2);
    }

}
