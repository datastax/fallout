# Extending Fallout

Fallout was designed to be easily extendable, so that it handles testing at any scale, on any environment. The API is flexible enough to allow teach Fallout how to install and configure new types of software, or use different underlying provisioning systems, without needing to change any core abstractions.

### Property Groups and Specs
The Property Group API consists of [PropertyGroup.java](../src/main/java/com/datastax/fallout/ops/PropertyGroup.java), [PropertySpec.java](../src/main/java/com/datastax/fallout/ops/PropertySpec.java), and [PropertySpecBuilder.java](../src/main/java/com/datastax/fallout/ops/PropertySpecBuilder.java).

PropertySpecs are used by Provisioners, Configuration Managers, Modules and (Artifact) Checkers to describe, validate, and parse user inputs. They are used to represent properties,
such as the number of nodes in a cluster, but also allow you to ensure a set of constraints,
such as that node count being > 0 and < 100, and passed as a parsable integer.

Since property specs are self describing we can use them to self-document and even auto create UI for your extensions!

PropertySpecs should be created with a PropertySpecBuilder. A PropertySpecBuilder provides an easy API for creating a PropertySpec, and defining its name, description,
default, validator, and parser. See the class definitions above for details.

Example PropertySpecs:
```java
    static final PropertySpec<String> cloudProviderSpec = PropertySpecBuilder.create(prefix)
            .name("cloud.provider")
            .description("The cloud provider to use")
            .options(ImmutableList.of("rightscale","ec2"))
            .defaultOf("rightscale")
            .required()
            .build();

    static final PropertySpec<Boolean> restartSpec = PropertySpecBuilder
            .createBool(prefix, true) // default value is true
            .name("restart")
            .description("Whether to restart")
            .build();

    static final PropertySpec<Integer> sshTimeout = PropertySpecBuilder
            .createInt(prefix, 30) // default timeout is 30 sec
            .name("timeout")
            .description("Timeout for ssh connection in seconds")
            .build();
```

PropertySpecs can also be **nested**.  This makes for prettier UI and simpler apis.

```java
    static final PropertySpec<String> parent = PropertySpecBuilder.createStr(prefix)
            .name("cloud.provider")
            .description("The cloud provider to use")
            .options(ImmutableList.of("rightscale","ec2"))
            .defaultOf("rightscale")
            .required()
            .build();

    static final PropertySpec<String> child = PropertySpecBuilder.createStr(prefix)
            .name("ec2.key")
            .description("EC2 secret key")
            .dependsOn(parent)    /* This property will only be used/shown if correct parent option is set */
            .category("ec2")      /* Category matches one of the parent options */
            .required()       /* Validation will only happen if parent is properly set */
            .build();
```

PropertySpecs allow using an **alias**, which basically means that if `cluster.name` in the below example was not set, it
will get its value from the PropertySpec `FalloutPropertySpecs.clusterNamePropertySpec.name()`.

```java
    static final PropertySpec<String> cassandraClusterName = PropertySpecBuilder.createStr(prefix, "[0-9a-zA-Z\\.\\-_]+")
            .name("cluster.name")
            .alias(FalloutPropertySpecs.clusterNamePropertySpec.name())
            .description("The name of the cluster")
            .required()
            .build();
```

For maintaining backward compatibility, PropertySpecs support the usage of **deprecatedName**. Lets say at the beginning
we had the below PropertySpec, which allows setting the **cassandra.version** to install.

```java
static final PropertySpec<String> cassandraVersion = PropertySpecBuilder.createStr(prefix)
            .name("cassandra.version")
            .required()
            .build();
```

However, at some point we decided that this PropertySpec can be used more generically for other product versions as well, such as **DSE** or **OpsCenter**.
Yet we don't want to break existing tests that still use **cassandra.version**.
In order to achieve this, we give the PropertySpec a new name **product.version** and use **deprecatedName** to indicate the **old** name of the PropertySpec.


```java
static final PropertySpec<String> productVersion = PropertySpecBuilder.createStr(prefix)
            .name("product.version")
            .deprecatedName(prefix + "cassandra.version")
            .required()
            .build();
```


Node and NodeGroups both use PropertyGroup to take in the set of properties
defined by users, when the Node or NodeGroup is built. These properties are
then validated against the PropertySpecs defined in the Provisioner or Configuration Manager used by the Node or NodeGroup.

```java
        PropertyGroup properties = new PropertyGroup();
        properties.put("fallout.provisioner.sshonly.host", "localhost");
        properties.put("fallout.system.user.name", "unittest");
        properties.put("fallout.system.user.privatekey", "none");
        properties.put("fallout.provisioner.sshonly.port", "19292");
        properties.put("fallout.provisioner.sshonly.password", "123");
        properties.put("test.config.foo", "abc");

        NodeGroup testGroup = NodeGroupBuilder
                .create()
                .withName("test group")
                .withProvisioner(provisioner)
                .withConfigurationManager(configurationManager)
                .withPropertyGroup(properties)
                .withNodeCount(10)
                .build();
```

### Writing Your Own Provisioner

The [Provisioner](../src/main/java/com/datastax/fallout/ops/Provisioner.java) is responsible for managing nodes. [See more](operations.md#provisioners).
When writing your own Provisioner, you implement all core api methods. All these operations are idempotent, asynchronous, and should be synchronized at the node/nodeGroup level. You can find examples of a properly implemented Provisioners in the [Provisioners directory](../src/main/java/com/datastax/fallout/ops/provisioner).

The following are a list of core methods that need implemented for each new Provisioner. Your Provisioner should extend the abstract base class [Provisioner.java](../src/main/java/com/datastax/fallout/ops/Provisioner.java), which for every `.\*Impl` method, implements a wrapper method that validates the state of the node(s) being acted upon, and returns a CompletableFuture.


Key Methods:
- `PropertySpec[] getPropertySpecs()` - Define the list of properties you will accept
- `boolean createImpl(NodeGroup nodeGroup)` - Create or Provision Nodes.  This should do the bare minimum to get all the nodes in the group up and running, as it will block other test runs from starting.
- `boolean prepareImpl(NodeGroup nodeGroup)` - Optional; should perform any additional work to get the nodes into a state where they're ready to be started by `startImpl`.
- `boolean startImpl(NodeGroup nodeGroup)` - Starts a stopped node group. Puts the node group into a running state.
- `boolean stopImpl(NodeGroup nodeGroup)` - Stops a started node group. Shuts down the node group.
- `NodeResponse executeImpl(Node node, String command)` - Runs a command on a node, returns the node's response.
- `boolean destroyImpl(NodeGroup nodeGroup)` - Destroys or unprovisions a set of nodes.
- `NodeGroup.State checkStateImpl(NodeGroup nodeGroup)` - Inspects the node group to determine its current state
- `CompletableFuture<Boolean> get/put(Node node, String remotePath, Path localPath, boolean deepCopy)` - Get, or put, respectively, files or directories from the node to the client

To use your Provisioner, you need to [declare it](extending.md#declaring-your-extensions).

### Writing Your Own Configuration Manager

The [Configuration Manager](../src/main/java/com/datastax/fallout/ops/ConfigurationManager.java) class (or CM) are designed just like Provisioners except they deal with configuring/unconfiguring software and starting/stopping services. [See more](operations.md#configuration_managers).

Much like Provisioner, all operations for a CM are idempotent and asynchronous. Also like Provisioner, the key .\*Impl methods have been wrapped in the base class. Your Configuration Manager should extend the abstract base class [ConfigurationManager.java](../src/main/java/com/datastax/fallout/ops/ConfigurationManager.java). You can find examples of properly implemented Configuration Managers in the [Configuration Managers directory](../src/main/java/com/datastax/fallout/ops/configmanagement).

Configuration Managers can depend on other Configuration Managers having previously run. These dependencies are represented as required Providers, declared by the `getRequiredProviders` method. The order in which Configuration Managers should operate in order to satisfy dependencies is handled automatically by the `MultiConfigurationManager`.

Key Methods:
- `PropertySpec[] getPropertySpecs()` - Define the list of properties you will accept
- `Optional<Product> product(NodeGroup serverGroup)` - The product this CM installs, if any
- `boolean configureImpl(NodeGroup nodeGroup)` - Configures all nodes in the node group, based on properties. Installs software, etc.
- `boolean unconfigureImpl(NodeGroup nodeGroup)` - Undoes this CM's configure step. Useful for test cleanup, or for cluster reuse.
- `boolean startImpl(NodeGroup nodeGroup)` - Start the configured software/services.
- `boolean stopImpl(NodeGroup nodeGroup)` - Stop the configured software/services.
- `NodeGroup.State checkStateImpl(NodeGroup nodeGroup)` - Inspects the node group to determine its current state
- `Set<Class<? extends Provider>> getRequiredProviders(PropertyGroup nodeGroupProperties)` - Declares all dependencies required to operate given the specified properties.
- `Set<Class<? extends Provider>> getAvailableProviders(PropertyGroup nodeGroupProperties)` - Declares all Providers created given the specified properties.

To use your Configuration Manager, you need to [declare it](extending.md#declaring-your-extensions).

### Writing Your Own Module
[Modules](../src/main/java/com/datastax/fallout/harness/Module.java) perform the bulk of the testing, failure injecting, and monitoring logic against C* or DSE. See [Modules](testing.md#modules).

Modules are a nice abstraction that prevents us from needing to do actual Jepsen scripting. Your module should extend the abstract base class [Module.java](../src/main/java/com/datastax/fallout/harness/Module.java). Your module should return a pair of an Operation.Type and an Object. The Operation.Type carries
high-level information about the Module's success (Type.ok for success, Type.fail for failure, and Type.info for unknown), and the Object field can be used for optional additional information in a freeform manner. For more on the Types that can be returned, see [Operation.java](../src/main/java/com/datastax/fallout/harness/Operation.java).

[See the implemented modules](../src/main/java/com/datastax/fallout/harness/modules).

Key Methods:
- `PropertySpec[] getPropertySpecs()` - Define the list of properties you will accept
- `void setup(Ensemble ensemble, PropertyGroup properties)` - Sets up your module prior to a test
- `void run(Ensemble ensemble, PropertyGroup properties)` - Starts your module, and performs its operations
- `void teardown(Ensemble ensemble, PropertyGroup properties)` - Clean up after your module after a test

A powerful feature available is history callbacks. Using the `addOnEmittedCallback(Consumer<Operation> onOperationEmitted)` method, a Module author can add a callback for each Operation in the history. This callback will get called for each existing Operation already in the history and also each new Operation emitted to the history. Operations emitted to the history by the Module itself will be excluded. Note that this callback may be called multiple times in parallel and may process operations out-of-order.

To use your Module, you need to [declare it](extending.md#declaring-your-extensions).

To use your Module, you need to [declare it](extending.md#declaring-your-extensions).

### Writing Your Own Checker
[Checkers](../src/main/java/com/datastax/fallout/harness/Checker.java) validate the results of a test.

Your checker should extend the Checker abstract base class.

To use your Checker, you need to [declare it](extending.md#declaring-your-extensions).

### Writing Your Own Artifact Checker
[Artifact Checkers](../src/main/java/com/datastax/fallout/harness/ArtifactChecker.java) validate the results of a test by examining particular artifacts,
 such as the **system.log** of a node running Cassandra.

Your artifact checker should extend the abstract [Artifact Checker](../src/main/java/com/datastax/fallout/harness/ArtifactChecker.java) base class.

To use your Artifact Checker, you need to [declare it](extending.md#declaring-your-extensions).

### Using Your Extensions

After you've extended Fallout, and added your new (Artifact) Checker(s), Module(s), Provisioner, and/or Configuration Manager, you are ready to use it in a test. Here is the sample yaml from earlier in these docs:
```YAML
namespace: cass-operator
nosqlbench_yaml: cql-iot.yaml
---
ensemble:
    server:
        node.count: 1
        local_files: # required for kubernetes manifests
            - url: https://raw.githubusercontent.com/datastax/fallout/master/examples/kubernetes/datastax-cass-operator-resources/ds-cass-operator-v1.yaml
              path: ds-cass-operator.yaml
            - url: https://raw.githubusercontent.com/datastax/fallout/master/examples/kubernetes/datastax-cass-operator-resources/kind-default-dc.yaml
              path: kind-cass-dc.yaml
            - url: https://raw.githubusercontent.com/datastax/fallout/master/examples/kubernetes/datastax-cass-operator-resources/rancher-local-path-storage.yaml
              path: rancher-local-storage.yaml
        provisioner:
            name: kind
        configuration_manager:
            - name: kubernetes_manifest
              properties:
                  manifest: <<file:rancher-local-storage.yaml>>
                  wait.strategy: FIXED_DURATION
                  wait.timeout: 30s
            - name: ds_cass_operator
              properties:
                  namespace: {{namespace}}
                  operator.manifest: <<file:ds-cass-operator.yaml>>
                  datacenter.manifest: <<file:kind-cass-dc.yaml>>
            - name: nosqlbench
              properties:
                  namespace: {{namespace}}
                  replicas: 1
    client: server

workload:
    phases:
        - init_schema:
              module: nosqlbench
              properties:
                  num_clients: 1
                  args:
                      - run
                      - type=cql
                      - yaml={{nosqlbench_yaml}}
                      - tags=phase:schema
        - benchmark:
              module: nosqlbench
              properties:
                  args:
                      - run
                      - type=cql
                      - yaml={{nosqlbench_yaml}}
                      - tags=phase:main
                      - cycles=100K
                      - errors=histogram
    checkers:
        nofail:
            checker: nofail
    artifact_checkers:
        hdr:
            artifact_checker: hdrtool
```

Using a new provisioner, or Configuration Manager is as easy as changing the yaml:

```YAML
namespace: cass-operator
nosqlbench_yaml: cql-iot.yaml
---
ensemble:
    server:
        node.count: 1
        local_files: # required for kubernetes manifests
            - url: https://raw.githubusercontent.com/datastax/fallout/master/examples/kubernetes/datastax-cass-operator-resources/ds-cass-operator-v1.yaml
              path: ds-cass-operator.yaml
            - url: https://raw.githubusercontent.com/datastax/fallout/master/examples/kubernetes/datastax-cass-operator-resources/kind-default-dc.yaml
              path: kind-cass-dc.yaml
            - url: https://raw.githubusercontent.com/datastax/fallout/master/examples/kubernetes/datastax-cass-operator-resources/rancher-local-path-storage.yaml
              path: rancher-local-storage.yaml
        provisioner:
            name: MyCustomProvisioner
        configuration_manager:
            - name: kubernetes_manifest
              properties:
                  manifest: <<file:rancher-local-storage.yaml>>
                  wait.strategy: FIXED_DURATION
                  wait.timeout: 30s
            - name: ds_cass_operator
              properties:
                  namespace: {{namespace}}
                  operator.manifest: <<file:ds-cass-operator.yaml>>
                  datacenter.manifest: <<file:kind-cass-dc.yaml>>
            - name: nosqlbench
              properties:
                replicas: 1
            - name: MyCustomConfigurationManager
    client: server

workload:
    phases:
        - init_schema:
              module: nosqlbench
              properties:
                  num_clients: 1
                  args:
                      - run
                      - type=cql
                      - yaml={{nosqlbench_yaml}}
                      - tags=phase:schema
        - benchmark:
              module: nosqlbench
              properties:
                  args:
                      - run
                      - type=cql
                      - yaml={{nosqlbench_yaml}}
                      - tags=phase:main
                      - cycles=1000
                      - errors=histogram
    checkers:
        nofail:
            checker: nofail
    artifact_checkers:
        hdr:
            artifact_checker: hdrtool
```

Make sure that the name() method of your Module, (Artifact) Checker, Provisioner, or Configuration Manager matches the string you are using to identify them in the test YAMLs. We match based on name(), not on the class name.

### Declaring Your Extensions

New Modules, Provisioners, (Artifact) Checkers, and Configuration Managers are loaded using ServiceLoader (`Utils.loadComponents`). The jar with your implementation of these base classes just needs to be added to Fallout's classpath, and you also need provider-configuration files in META-INF/services in their jar as described here https://docs.oracle.com/javase/7/docs/api/java/util/ServiceLoader.html, or use [`@AutoService`](https://github.com/google/auto/tree/master/service) as the fallout codebase does.
