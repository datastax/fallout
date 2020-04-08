# Fallout Documentation

## Introduction
Fallout aims to improve the ability to test Apache Cassandra and other products, distributed or otherwise. Unit tests provide small-scale testing ability in-tree and are a useful first check. The [dtests](https://github.com/apache/cassandra-dtest) offer local functional testing useful for catching cluster-level issues, but they (by design) lack environmental flexibility and sophisticated manipulation of failure conditions. [cstar_perf](https://github.com/datastax/cstar_perf) makes powerful and flexible automated performance testing feasible, but its automation is not designed for flexible service configuration or easily scaling from laptop-level to cloud-provisioned instances. It also isn't designed for easy verification of sophisticated correctness conditions. Fallout fills these gaps in testing by making it possible to run local or large-scale tests focusing on correctness and performance under complex, potentially random environmental and operational conditions. To mitigate further test tool proliferation, Fallout should be easily extensible and well-documented.

Fallout is a server built using the Dropwizard framework. This server can be made available as a service or run locally for development; it provides both a REST API and a Web UI.

Fallout is capable of provisioning and configuring resources to run a test. A test can run concurrent, multi-phase workloads and check the results of these workloads for correctness conditions. All of the above components are pluggable; in the event that the above model is insufficient, an escape hatch is available in the form of Jepsen scripting.

## [Fallout Operations](operations.md)
* [Ensembles, NodeGroups, and Nodes](operations.md#ensembles-nodegroups-and-nodes)
* [Understanding the Fallout Lifecycle](operations.md#understanding-the-fallout-lifecycle)
* [Provisioners](operations.md#provisioners)
* [Configuration Managers](operations.md#configuration-managers)
* [Providers](operations.md#providers)

## [Fallout Testing](testing.md)
* [A Brief Explanation of Jepsen](testing.md#a-brief-explanation-of-jepsen)
* [Modules](testing.md#modules)
* [Module Lifetimes](testing.md#module-lifetimes)
* [Phases](testing.md#phases)
* [Checkers](testing.md#checkers)
* [Artifact Checkers](testing.md#artifact-checkers)
* [Writing Your First Test](testing.md#writing-your-first-test)
* [Running Your First Test](testing.md#running-your-first-test)
* [Templated Tests](testing.md#templated-tests)

## [Extending Fallout](extending.md)
* [Property Groups and Specs](extending.md#property-groups-and-specs)
* [Writing Your Own Provisioner](extending.md#writing-your-own-provisioner)
* [Writing Your Own Configuration Manager](extending.md#writing-your-own-configuration-manager)
* [Writing Your Own Module](extending.md#writing-your-own-module)
* [Writing Your Own Checker](extending.md#writing-your-own-checker)
* [Writing Your Own Artifact Checker](extending.md#writing-your-own-artifact-checker)
* [Using Your Extensions](extending.md#using-your-extensions)

## [REST API](rest_api.md)
* [Creating and Running Tests](rest_api.md#creating-and-running-tests)
* [Performance test comparisons](rest_api.md#performance-test-comparisons)

## [Deleting Tests](deleting_tests_and_orphaned_artifacts.md)

There are a [number of examples](../examples), which are individually linked and documented.
