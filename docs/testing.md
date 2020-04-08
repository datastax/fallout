# Fallout Testing
If you've already read the earlier sections, you've learned why we chose to create Fallout and how Fallout manages resources needed to run your test. Now, we'll talk a little about how to write and run a test using Fallout. In the process, you'll get a look behind-the-scenes at how Fallout runs a test and the flexibility available.

## A Brief Explanation of Jepsen
When designing Fallout, we wanted to support two different ways of writing tests. The first type of test is declarative. In our experience, scripting tests is often unnecessary and results in error-prone, cargo-culted snippets being copy-pasted across files. While this is workable and sometimes necessary, the frequent regressions in such environment undermine confidence in testing and impose a significant workload in maintaining the tests.

These declarative tests should have the ability to run a test in sequential [phases](testing.md#phases). During a phase, we want one or more testing modules to be able to run in parallel. This simple model allows us to test many scenarios but remains easy to understand.

At the same time, we recognize that there will be some tests that are not suitable to a declarative model. This includes tests that should react to some out-of-band component, tests with complex scheduling requirements, and tests with complex state management across phases. It would be unfortunate to duplicate the work already done in Fallout to provision and configure resources in another environment that could run these flexible tests.

From these requirements, it becomes clear that we need a testing API that supports both declarative and scripting modes. We observed that we could fulfill these needs by wrapping Jepsen. Moreover, since we've developed tests in Jepsen at DataStax already, we have a library of tests from which to draw and experience at debugging this test runner. Fallout avoids requiring users to learn Jepsen by wrapping its internals with the simpler `Module` and `Checker` abstractions. That said, learning its internals may be useful in developing for Fallout. If you're not interested in these mostly-hidden details, feel free to skip to [Modules](#Modules).

Jepsen is a distributed testing library developed by Kyle Kingsbury. It allows users to easily write tests in Clojure and offers some environmental orchestration. After these tests complete, a checker is used to analyze the results. These Clojure tests offer the scripting API we want. The library is general enough that we can easily convert declarative YAML test specifications into Jepsen tests. This integration is relatively simple because Fallout and Jepsen both run on the JVM.

Jepsen uses a few key abstractions: tests, *OS*s, *DB*s, *Clients*, *generators*, *nemeses*, and *checkers*. We've extended our fork of Jepsen with an additional abstraction called *conductors*. These are covered in detail in a presentation Joel Knighton gave at Summit available on [Slideshare](http://www.slideshare.net/jkni/testing-cassandra-guarantees-under-diverse-failure-modes-with-jepsen-53168992).

A test is a Clojure map with entries for the above abstractions. When run, Jepsen accesses a set of nodes over SSH, configures the operating system and database as defined in the *DB* and *OS* implementations, and starts threads to run the client. These client threads take operations from generators that specify behaviors. For example, a CAS client would get an operation from the generator with information specifying which values to read, write, or CAS. After executing the operation, the client associates information on the operation and returns this augmented operation. Both the original operation and this returned operation will be appended to the history. In parallel, a nemesis runs, taking operations and incurring failure conditions as specified. In implementation, the nemesis is just another client. At DataStax, we extended the idea of a nemesis to conductors. This allows more than one nemesis to run at the same time, and it was implemented to enable testing during bootstraps and decommissions. After all client threads have stopped (which is triggered by a null returned from the generator), the test is complete. Then, a checker analyzes the history built during the test and determines whether the test has passed or failed. These checkers are composable so that multiple properties/invariants may be checked against the history.

In order to turn a declarative YAML into a test, we transform the workload into a Jepsen test of the above form. First, we take the modules listed and instantiate a copy of each. We associate each of these modules as a conductor and create a no-op client. Then, we create a generator that uses synchronization via a latch to trigger each module in a phase exactly once and only trigger the next phase after all modules in the current phase have returned. At runtime, we take this test map, provision and configure the node groups as specified, associate these provisioned resources as keys in the map, and pass it to the Jepsen test runner.

## Modules
[Modules](../src/main/java/com/datastax/fallout/harness/Module.java) are the first key abstraction in the testing API. A module runs during the test to perform one ore more actions. For example, one might write a module wrapping Cassandra stress. One might also want a repair module or a module simulating some failure condition, such as a flaky network or a bad disk.

As with most test-specific abstractions, the Module abstract class is defined in the [`java.com.datastax.fallout.harness`](../src/main/java/com/datastax/fallout/harness) package. The Module abstract class implements the `Client` interface defined in Jepsen; Jepsen has an understanding of clients and nemeses, but both of these implement the `Client` interface.

We use a wrapper abstract class because the `Client` interface generated by Clojure code lacks type information and passes in the whole test-map. To isolate Module implementors from this complexity, the Module class implements stub version of these methods that cleans the input and presents Ensembles and PropertyGroups as arguments to the methods to be implemented.

The principal methods to be implemented when extending Module are fairly simple.
```Java
Class<? extends Provider>[] getRequiredProviders();

List<Product> getSupportedProducts();

void setup(Ensemble ensemble, PropertyGroup properties){};

void run(Ensemble ensemble, PropertyGroup properties){};

void teardown(Ensemble ensemble, PropertyGroup properties){};
```
The `getRequiredProviders` return value will be checked before a test is run to ensure that the ConfigurationManagers being used in the test can supply these Providers. If not, the test will not be run, saving the creation of costly resources if the test cannot possibly run.

The `getSupportedProducts` return value will be checked before a test is run to ensure that the product being tested, as established by the server groups' configuration managers, is in this list. If not, the test will not be run, to prevent the module from possibly subtly breaking the test.

The `setup` method is called before the run method and can be used to setup any necessary resources. At this point, all services specified in the ensemble should be available.

The `run` method will be triggered once the test reaches the Module's phase. This is where the Module should exercise the system under test.

The `teardown` method is called after run method and should be used to perform any cleanup necessary.

The timing of the `setup` and `teardown` methods can be controlled by setting the instance variable `useGlobalSetupTeardown` on a Module. This variable defaults to false. When false, a Module will run `setup` immediately before the `run` method and will run `teardown` immediately after the `run` method. When true, a Module will run `setup` and `teardown` at the start and end of the whole test, respectively.

A Module appends operations to the runtime history using the following methods:
```java
void emit(Operation.Type type);

void emit(Operation.Type type, Object value);

void emit(Operation.Type type, MediaType mimeType, Object value);
```

Some helper methods exist to report common operations / events:
```java
void emitInfo(String message);

void emitOk(String message);

void emitError(String message);

void emitFail(String message);
```

In order of increasing specificity, Modules may emit an operation of a given type, an operation of a given type with an associated value, or an operation of a given type with a value tagged by a MediaType. MediaTypes can be useful for filtering a history.

At the start of a module's run method, a start operation is automatically emitted to the history. At the end of a module's'
run method, an end operation is automatically emitted to the history. At least one operation must be manually emitted to the history during the run method. Usually, this will be one or more pairs of an invoke method indicating the start of some action and either an ok, fail, or info method indicating the completion of an action, depending on whether it succeeds, fails, or has unknown status, respectively.

A Module may also add callbacks for when an Operation is appended to the history. By using `addOnEmittedCallback(Consumer<Operation opOperationEmitted)`, one can add this callback. It will be called for each existing Operation already in the history and also for each new Operation emitted to the history. Operations emitted to the history by the Module itself will be excluded. Note that this callback may be called multiple times in parallel and may process operations out-of-order.

## Module Lifetimes

Modules can have a lifetime of `run_once`, or `run_to_end_of_phase`; the latter means they'll keep running until all other `run_once` modules in the phase have completed.  Some modules have `run_to_end_of_phase` lifetime hard-coded; these are identified as having a **phase lifetime** in the online module docs.  All other modules have a **dynamic lifetime**, and the behaviour can be set with the `lifetime` property.

## Phases
Fallout tests are constructed as a list of phases, which are the basic unit of concurrency. A phase is a collection of modules, all of which run in parallel.  Once all of the modules in a phase have completed, Fallout will move to the next phase in the list.  This simple model allows us to test many scenarios but remains easy to understand; it looks like this in the YAML test definition syntax (dummy is a hypothetical module that does nothing useful):

```yaml
workload:
  phases:
    - a: {module: dummy} # | This is a single phase; it's a list item containing
      b: {module: dummy} # | a YAML map, each key of which is a module.
    - c: {module: dummy}
      d: {module: dummy}
```

(Normally, we'd write modules like this:
```
    - a:
        module: dummy
      b:
        module: dummy
```
...but we're using YAML's inline syntax for conciseness)

The above example says "run modules a and b concurrently; when they are finished, run c and d concurrently"; for the rest of the documentation we'll use a terser but accurate form: "(a and b) then (c and d)".

As well as modules, a phase can contain lists of other phases.  This allows us to model more complicated test scenarios, such as one client performing steps a, b, c sequentially, while another client performs steps d, e, f, and then another step, g, is run at the end; i.e. "((a then b then c) and (d then e then f)) then g":

```yaml
workload:
  phases:
    - one_client:
        - a: {module: dummy} # Instead of a single module we have a list
        - b: {module: dummy} # of phases here.  In this case, each phase
        - c: {module: dummy} # contains a single module.
      another_client:
        - d: {module: dummy}
        - e: {module: dummy}
        - f: {module: dummy}
    - g: {module: dummy}
```

There's no limit to:

* the number of modules or lists of phases in a single phase;
* the number of phases in a list of phases;
* the nesting depth.

The keys naming modules (in the above example these are `a, b, c, d, e, f, g`) must be unique across the whole test.

Modules with a `run_to_end_of_phase` lifetime will await all `run_once` modules in their phase, even those nested in subphases.

A complete example is here: [`examples/9-nested-subphases.yaml`](../examples/nested-subphases.yaml).

## Checkers
Modules allow a user to perform actions against systems under test, but they do not provide an ability to understand correctness. For the types of tests being run under Fallout, correctness conditions will often span across the actions of multiple modules.

In Jepsen, this post-run analysis takes place when a [`Checker`](../src/main/java/com/datastax/fallout/harness/Checker.java) is run. A `Checker` receives the full operation history of a test as input. It can then run arbitrary validation against that input. Multiple checkers may be used in a single test; the test passes if and only if all checkers pass. In the test results, granular output from each checker will be available.

As in the case of modules, we use a thin wrapper abstract class around Jepsen's checker interface, available in the `java.com.datastax.fallout.harness`. This wrapper converts the history from a Clojure seq of freeform maps to a List<[`Operation`](../src/main/java/com/datastax/fallout/harness/Operation.java)>. The Operation class wraps these freeform maps and provides easy access to the `Operation.Type` (an enum consisting of `info`, `invoke`, `ok`, and `fail` options) and `Value`, a free-form field consisting of an Object. Usually, `Type` should reflect the success of an operation, and `Value` is extra information provided to or extracted from the Module. For example, a Module implementing CAS through LWT might return the actual value read on a failed CAS.

When extending Checker, the principal method is:
```Java
boolean check(Ensemble ensemble, Collection<Operation> history);
```

This `check` method will be called after a test is torndown to inspect the history and must return a boolean, where `true` indicates that the history is valid and `false` indicates that the history is invalid.

A very basic checker is the [`NoFailChecker`](../src/main/java/com/datastax/fallout/harness/checkers/NoFailChecker.java). This Checker takes a history and confirms that it contains no failed Operations.

In some cases, it may be useful for a Checker to use the MediaType associated with each Operation in the history. This can be access using the `getMediaType()` method on the Operation.

## Artifact Checkers

An `Artifact Checker` receives as input the root directory containing all artifacts of a test. It can then run arbitrary validation against that input. Multiple artifact checkers may be used in a single test; the test passes if and only if all artifact checkers pass. In the test results, granular output from each artifact checker will be available.

When extending the abstract base class `Artifact Checker`, the principal method is:
```java
boolean validate(Ensemble ensemble, Path rootArtifactLocation);
```

This `validate(...)` method will be called after shutting down the `ensemble` and only in case artifacts are available. The method will return `true` if validation was successful and `false` otherwise.

A very basic artifact checker is the [`SystemLogChecker`](../src/main/java/com/datastax/fallout/harness/artifact_checkers/SystemLogChecker.java). This artifact checker will validate the **system.log** by checking if a particular error pattern can be found in the log.


## Writing Your First Test
As is often the case, these concepts become clearer with an example. Let's look at the [default example test](../src/main/resources/com/datastax/fallout/service/resources/server/default-test-definition.yaml).

```YAML
namespace: cass-operator
nosqlbench_yaml: cql-iot.yaml
---
ensemble:
    server:
        node.count: 1
        local_files: # required for kubernetes manifests
            - url: https://raw.githubusercontent.com/datastax/fallout/oss-fallout/examples/kubernetes/datastax-cass-operator-resources/ds-cass-operator-v1.yaml
              path: ds-cass-operator.yaml
            - url: https://raw.githubusercontent.com/datastax/fallout/oss-fallout/examples/kubernetes/datastax-cass-operator-resources/kind-default-dc.yaml
              path: kind-cass-dc.yaml
            - url: https://raw.githubusercontent.com/datastax/fallout/oss-fallout/examples/kubernetes/datastax-cass-operator-resources/rancher-local-path-storage.yaml
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
This test has two major top-level components. The first is the `ensemble` entry. This specifies how to configure the four types of NodeGroups in the ensemble. These NodeGroups will be created before the test is run and destroyed at the end of the test. In this example, the server node group will be provisioned locally with KIND and configured using the DataStaxCassOperator, KubernetesManifest, and NoSqlBench Configuration Managers . The remaining NodeGroups are omitted. As can be seen, the entry for a specific NodeGroup may be a map of options or a pointer to an existing NodeGroup. In the case of a map of options, these resources will be provisioned and configured as necessary. In the case of a pointer, these existing resources will be used. Available options for the pointer are any already configured Ensemble group (i.e. server, client, controller, observer) or `local`/`none`, both of which use the server running the Fallout API.

The final component is the `workload`. The workload describes what actions to run during the test. The `phases` entry should map to a map. Each entry in this map should be a unique alias which maps to another map. This alias is used so that multiple instances of the same Module can be present in the same phase. Entries in the alias map should include the module name and a map of properties to pass to the Module. All of the Modules in a given list entry will run in parallel. The `checkers` / `artifact_checkers` entry should map to a map. The keys in this map will be (Artifact) Checker aliases; the entry for a given key should include an (artifact) checker entry and a properties entry. The (artifact) checker key should map to an (Artifact) Checker name. The properties entry will get passed to this (artifact) checker instance.

## Running Your First Test

You can create and submit a test via the UI.  Alternatively, fallout can be controlled via its [REST API](rest_api.md).

## Templated tests

Test definitions can be parameterized, with the parameters being specified on each run; see the [README](../examples/templating/README.md) in [`examples/templating`](../examples/templating).
