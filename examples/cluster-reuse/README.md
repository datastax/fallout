# Cluster Reuse

Fallout is capable of creating and reusing clusters.

When a cluster is marked for reuse, it will be exempt from all tear down actions except those related to collecting artifacts. This means all services running the end of a test will continue to run even after the test has finished.

There is one way to setup a cluster for reuse: as an artifact of a test.

## Saving a Cluster After a Test

To save a cluster at the end of a test run, the cluster must meet the following conditions:
* the Provisioner `nameSpec` must be set
* the `mark_for_reuse` property must be `true`

If all of these conditions are met, the cluster will be reusable.

## Reusing a Created Cluster

To reuse a cluster saved after a test run, one must simply use the same NodeGroup yaml the cluster was created with. In particular, the Provisioner `nameSpec` must be the same.

Any properties which differ between creating the cluster and reusing the cluster will not change the properties on the cluster. However, Fallout will treat the cluster as if it had those settings, erroneously or not.

## Destroying a Cluster

To destroy a cluster at the end of a test run, remove the `mark_for_reuse` property from the NodeGroup yaml. The cluster will be destroyed as normal.
