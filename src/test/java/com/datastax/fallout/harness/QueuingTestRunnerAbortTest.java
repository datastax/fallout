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
package com.datastax.fallout.harness;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Uninterruptibles;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;

import com.datastax.fallout.components.fakes.FakeProvisioner;
import com.datastax.fallout.components.impl.FakeModule;
import com.datastax.fallout.ops.Ensemble;
import com.datastax.fallout.ops.Node;
import com.datastax.fallout.ops.NodeGroup;
import com.datastax.fallout.ops.PropertyGroup;
import com.datastax.fallout.ops.Provisioner;
import com.datastax.fallout.ops.ResourceRequirement;
import com.datastax.fallout.ops.commands.CommandExecutor;
import com.datastax.fallout.ops.commands.NodeResponse;
import com.datastax.fallout.runner.CheckResourcesResult;
import com.datastax.fallout.runner.QueuingTestRunner;
import com.datastax.fallout.service.FalloutConfiguration;
import com.datastax.fallout.service.core.Test;
import com.datastax.fallout.service.core.TestRun;
import com.datastax.fallout.util.Duration;
import com.datastax.fallout.util.ScopedLogger;

import static com.datastax.fallout.assertj.Assertions.assertThat;
import static com.datastax.fallout.harness.QueuingTestRunnerAbortTest.Method.*;
import static com.datastax.fallout.harness.QueuingTestRunnerAbortTest.MethodResult.FAILS;
import static com.datastax.fallout.harness.QueuingTestRunnerAbortTest.MethodResult.SUCCEEDS;
import static com.datastax.fallout.harness.QueuingTestRunnerAbortTest.MethodResult.THROWS;

@Timeout(value = 10, unit = TimeUnit.SECONDS)
public class QueuingTestRunnerAbortTest extends TestRunnerTestHelpers.QueuingTestRunnerTest<FalloutConfiguration>
{
    private static final ScopedLogger logger = ScopedLogger.getLogger(QueuingTestRunnerAbortTest.class);

    private void await(CountDownLatch latch)
    {
        Uninterruptibles.awaitUninterruptibly(latch);
    }

    private CountDownLatch methodStarted = new CountDownLatch(1);
    private CountDownLatch methodContinue = new CountDownLatch(1);

    enum Method
    {
        PROVISIONER_GET_RESOURCE_REQUIREMENTS, PROVISIONER_RESERVE, PROVISIONER_CREATE, PROVISIONER_PREPARE,
        PROVISIONER_START,
        MODULE_RUN, EXECUTE, EXECUTE_SUCCESS, EXECUTE_FAILURE, NODE_EXECUTION_KILLED, NODE_EXECUTION_NOT_KILLED,
        PROVISIONER_STOP, PROVISIONER_DESTROY
    }

    enum MethodResult
    {
        SUCCEEDS, FAILS, THROWS
    }

    private boolean methodResult()
    {
        switch (methodResult)
        {
            case SUCCEEDS:
                return true;
            case FAILS:
                return false;
            default:
                throw new RuntimeException("bang!");
        }
    }

    private final List<Method> calledMethods = new ArrayList<>();

    enum ExpectedAbortMode
    {
        NO_ABORT,
        ABORT,
        ABORT_WITH_KILL
    }

    public ExpectedAbortMode expectedAbortMode;

    private ExpectedAbortMode logAndMaybeSignalAndWaitOnce(Method method)
    {
        calledMethods.add(method);
        if (method == abortDuringMethod && --abortDuringMethodNumber == 0)
        {
            abortDuringMethod = null;
            return logger.withScopedInfo("signalling start of to-be-aborted method {} and waiting before returning {}",
                method, expectedAbortMode).get(() -> {
                    methodStarted.countDown();
                    await(methodContinue);
                    return expectedAbortMode;
                });
        }
        return ExpectedAbortMode.NO_ABORT;
    }

    private void waitForExitCodeAndAddToCalledMethods(NodeResponse nodeResponse)
    {
        final Optional<Integer> exitCode =
            nodeResponse.doWait().withCheckInterval(Duration.milliseconds(100)).forExitCode();

        if (exitCode.isPresent())
        {
            switch (exitCode.get())
            {
                case 0:
                    calledMethods.add(EXECUTE_SUCCESS);
                    break;
                case FakeNodeResponse.EXITCODE_KILL_EXPECTED_AND_RECEIVED:
                    calledMethods.add(NODE_EXECUTION_KILLED);
                    break;
                case FakeNodeResponse.EXITCODE_KILL_EXPECTED_AND_NOT_RECEIVED:
                    calledMethods.add(NODE_EXECUTION_NOT_KILLED);
                    break;
                default:
                    calledMethods.add(EXECUTE_FAILURE);
                    break;
            }
        }
        else
        {
            throw new RuntimeException("Unexpected timeout");
        }
    }

    private void executeCommand(NodeGroup nodeGroup)
    {
        CommandExecutor commandExecutor = nodeGroup.getProvisioner().getCommandExecutor();
        waitForExitCodeAndAddToCalledMethods(commandExecutor.local(nodeGroup.logger(), "bogus").execute());
    }

    private void executeCommand(Node node)
    {
        waitForExitCodeAndAddToCalledMethods(node.execute("bogus"));
    }

    class AbortModule extends FakeModule
    {
        @Override
        public void run(Ensemble ensemble, PropertyGroup properties)
        {
            emit(Operation.Type.invoke);

            logAndMaybeSignalAndWaitOnce(MODULE_RUN);

            final Node client = ensemble.getClientGroup("client").getNodes().get(0);

            // Execute two commands, so we can test what happens when the first is aborted
            executeCommand(client);
            executeCommand(client);

            emit(Operation.Type.ok);
        }
    }

    private class FakeNodeResponse extends NodeResponse
    {
        private CompletableFuture<Integer> exitCodeFuture;
        private final CountDownLatch killLatch;

        public final static int EXITCODE_KILL_EXPECTED_AND_RECEIVED = 137;
        public final static int EXITCODE_KILL_EXPECTED_AND_NOT_RECEIVED = 222;

        FakeNodeResponse(Node owner, final int exitCode, Logger logger)
        {
            super(owner, "fake", logger);
            killLatch = new CountDownLatch(1);
            exitCodeFuture = CompletableFuture.supplyAsync(() -> {
                if (logAndMaybeSignalAndWaitOnce(EXECUTE) == ExpectedAbortMode.ABORT_WITH_KILL)
                {
                    if (!Uninterruptibles.awaitUninterruptibly(killLatch, 1, TimeUnit.SECONDS))
                    {
                        return EXITCODE_KILL_EXPECTED_AND_NOT_RECEIVED;
                    }
                    return EXITCODE_KILL_EXPECTED_AND_RECEIVED;
                }
                return exitCode;
            });
        }

        @Override
        protected InputStream getOutputStream() throws IOException
        {
            return new ByteArrayInputStream(new byte[0]);
        }

        @Override
        protected InputStream getErrorStream() throws IOException
        {
            return new ByteArrayInputStream(new byte[0]);
        }

        @Override
        public int getExitCode()
        {
            return exitCodeFuture.join();
        }

        public boolean isCompleted()
        {
            return exitCodeFuture.isDone();
        }

        @Override
        public void doKill()
        {
            logger.info("killing node response");
            killLatch.countDown();
        }
    }

    private class FakeCommandExecutor implements CommandExecutor
    {
        @Override
        public NodeResponse executeLocally(Node owner, String command, Map<String, String> environment,
            Optional<Path> workingDirectory)
        {
            return new FakeNodeResponse(owner, methodResult() ? 0 : 1, owner.logger());
        }

        @Override
        public NodeResponse executeLocally(Logger logger, String command, Map<String, String> environment,
            Optional<Path> workingDirectory)
        {
            return new FakeNodeResponse(null, methodResult() ? 0 : 1, logger);
        }
    }

    class AbortProvisioner extends FakeProvisioner
    {
        @Override
        public Optional<ResourceRequirement> getResourceRequirements(NodeGroup nodeGroup)
        {
            logAndMaybeSignalAndWaitOnce(PROVISIONER_GET_RESOURCE_REQUIREMENTS);
            return Optional.empty();
        }

        @Override
        protected CheckResourcesResult reserveImpl(NodeGroup nodeGroup)
        {
            logAndMaybeSignalAndWaitOnce(PROVISIONER_RESERVE);
            executeCommand(nodeGroup);
            return CheckResourcesResult.fromWasSuccessful(methodResult());
        }

        @Override
        protected CheckResourcesResult createImpl(NodeGroup nodeGroup)
        {
            logAndMaybeSignalAndWaitOnce(PROVISIONER_CREATE);
            executeCommand(nodeGroup);
            return CheckResourcesResult.fromWasSuccessful(methodResult());
        }

        @Override
        protected boolean prepareImpl(NodeGroup nodeGroup)
        {
            logAndMaybeSignalAndWaitOnce(PROVISIONER_PREPARE);
            executeCommand(nodeGroup);
            return methodResult();
        }

        @Override
        protected boolean startImpl(NodeGroup nodeGroup)
        {
            logAndMaybeSignalAndWaitOnce(PROVISIONER_START);
            executeCommand(nodeGroup);
            return methodResult();
        }

        @Override
        protected boolean stopImpl(NodeGroup nodeGroup)
        {
            logAndMaybeSignalAndWaitOnce(PROVISIONER_STOP);
            executeCommand(nodeGroup);
            return methodResult();
        }

        @Override
        protected boolean destroyImpl(NodeGroup nodeGroup)
        {
            logAndMaybeSignalAndWaitOnce(PROVISIONER_DESTROY);
            executeCommand(nodeGroup);
            return methodResult();
        }

        @Override
        protected NodeResponse executeImpl(Node node, String command)
        {
            return new FakeNodeResponse(node, methodResult() ? 0 : 1, node.logger());
        }
    }

    private void abortsTestDuring(
        Method abortMethod, TestRun.State expectedFailedDuring,
        List<Method> expectedMethods)
    {
        final Test test = makeTest("abort-fake.yaml");
        final TestRun testRun = testRunFactory.makeTestRun(test);
        final CompletableFuture<TestRun> completedTestRun = new CompletableFuture<>();

        expectedAbortMode = expectedMethods.contains(NODE_EXECUTION_KILLED) ?
            ExpectedAbortMode.ABORT_WITH_KILL : ExpectedAbortMode.ABORT;

        try (QueuingTestRunner testRunner =
            testRunnerBuilder()
                .modifyComponentFactory(componentFactory -> componentFactory
                    .mockAll(Provisioner.class, AbortProvisioner::new)
                    .mockAll(Module.class, AbortModule::new))
                .modifyActiveTestRunFactory(activeTestRunFactory -> activeTestRunFactory
                    .withCommandExecutorFactory(FakeCommandExecutor::new))
                .withTestRunCompletionCallback(completedTestRun::complete)
                .build())
        {
            testRunner.queueTestRun(testRun);

            if (abortMethod != null)
            {
                logger.withScopedInfo("waiting for method {} to start before aborting", abortMethod).run(
                    () -> await(methodStarted));
                logger.withScopedInfo("aborting {}", abortMethod).run(() -> testRunner.abortTestRun(testRun));
                methodContinue.countDown();
            }

            assertThat(completedTestRun.join())
                .isEqualTo(testRun)
                .hasState(abortMethod != null ? TestRun.State.ABORTED : TestRun.State.PASSED)
                .hasFailedDuring(expectedFailedDuring);
        }

        assertThat(calledMethods).isEqualTo(expectedMethods);
        assertThat(testRunnerJobQueue.hasNoRequeuedJobs()).isTrue();
    }

    private static List<Method> executesCommand()
    {
        return List.of(EXECUTE, EXECUTE_SUCCESS);
    }

    private static List<Method> killsCommand()
    {
        return List.of(EXECUTE, NODE_EXECUTION_KILLED);
    }

    public static Object[][] params()
    {
        return new Object[][] {
            // MODULE_RUN appears twice in a non-aborted test, since abort-fake.yaml contains two phases in which an instance
            // of fake module is called.  When we abort during the first MODULE_RUN, then we don't see the second call.
            new Object[] {1, null, SUCCEEDS, null,
                ImmutableList.builder()
                    .add(PROVISIONER_GET_RESOURCE_REQUIREMENTS)
                    .add(PROVISIONER_RESERVE).addAll(executesCommand())
                    .add(PROVISIONER_CREATE).addAll(executesCommand())
                    .add(PROVISIONER_PREPARE).addAll(executesCommand())
                    .add(PROVISIONER_START).addAll(executesCommand())
                    .add(MODULE_RUN).addAll(executesCommand()).addAll(executesCommand())
                    .add(MODULE_RUN).addAll(executesCommand()).addAll(executesCommand())
                    .add(PROVISIONER_STOP).addAll(executesCommand())
                    .add(PROVISIONER_DESTROY).addAll(executesCommand())
                    .build()},

            new Object[] {1, PROVISIONER_GET_RESOURCE_REQUIREMENTS, SUCCEEDS, TestRun.State.CHECKING_RESOURCES,
                List.of(PROVISIONER_GET_RESOURCE_REQUIREMENTS)},
            new Object[] {1, PROVISIONER_GET_RESOURCE_REQUIREMENTS, FAILS, TestRun.State.CHECKING_RESOURCES,
                List.of(PROVISIONER_GET_RESOURCE_REQUIREMENTS)},
            new Object[] {1, PROVISIONER_GET_RESOURCE_REQUIREMENTS, THROWS, TestRun.State.CHECKING_RESOURCES,
                List.of(PROVISIONER_GET_RESOURCE_REQUIREMENTS)},

            new Object[] {1, PROVISIONER_RESERVE, SUCCEEDS, TestRun.State.RESERVING_RESOURCES,
                ImmutableList.builder()
                    .add(PROVISIONER_GET_RESOURCE_REQUIREMENTS)
                    .add(PROVISIONER_RESERVE).addAll(executesCommand())
                    .add(PROVISIONER_DESTROY).addAll(executesCommand())
                    .build()},

            new Object[] {1, EXECUTE, SUCCEEDS, TestRun.State.RESERVING_RESOURCES,
                ImmutableList.builder()
                    .add(PROVISIONER_GET_RESOURCE_REQUIREMENTS)
                    .add(PROVISIONER_RESERVE).addAll(executesCommand())
                    .add(PROVISIONER_DESTROY).addAll(executesCommand())
                    .build()},

            new Object[] {1, PROVISIONER_CREATE, SUCCEEDS, TestRun.State.ACQUIRING_RESOURCES,
                ImmutableList.builder()
                    .add(PROVISIONER_GET_RESOURCE_REQUIREMENTS)
                    .add(PROVISIONER_RESERVE).addAll(executesCommand())
                    .add(PROVISIONER_CREATE).addAll(executesCommand())
                    .add(PROVISIONER_DESTROY).addAll(executesCommand())
                    .build()},

            new Object[] {2, EXECUTE, SUCCEEDS, TestRun.State.ACQUIRING_RESOURCES,
                ImmutableList.builder()
                    .add(PROVISIONER_GET_RESOURCE_REQUIREMENTS)
                    .add(PROVISIONER_RESERVE).addAll(executesCommand())
                    .add(PROVISIONER_CREATE).addAll(executesCommand())
                    .add(PROVISIONER_DESTROY).addAll(executesCommand())
                    .build()},

            new Object[] {1, PROVISIONER_PREPARE, SUCCEEDS, TestRun.State.SETTING_UP,
                ImmutableList.builder()
                    .add(PROVISIONER_GET_RESOURCE_REQUIREMENTS)
                    .add(PROVISIONER_RESERVE).addAll(executesCommand())
                    .add(PROVISIONER_CREATE).addAll(executesCommand())
                    .add(PROVISIONER_PREPARE, EXECUTE_FAILURE)
                    .add(PROVISIONER_DESTROY).addAll(executesCommand())
                    .build()},

            new Object[] {3, EXECUTE, SUCCEEDS, TestRun.State.SETTING_UP,
                ImmutableList.builder()
                    .add(PROVISIONER_GET_RESOURCE_REQUIREMENTS)
                    .add(PROVISIONER_RESERVE).addAll(executesCommand())
                    .add(PROVISIONER_CREATE).addAll(executesCommand())
                    .add(PROVISIONER_PREPARE).addAll(killsCommand())
                    .add(PROVISIONER_DESTROY).addAll(executesCommand())
                    .build()},

            new Object[] {1, PROVISIONER_START, SUCCEEDS, TestRun.State.SETTING_UP,
                ImmutableList.builder()
                    .add(PROVISIONER_GET_RESOURCE_REQUIREMENTS)
                    .add(PROVISIONER_RESERVE).addAll(executesCommand())
                    .add(PROVISIONER_CREATE).addAll(executesCommand())
                    .add(PROVISIONER_PREPARE).addAll(executesCommand())
                    // The abort happens _after_ the PROVISIONER_START call is recorded
                    // but _before_ the command is executed, so the command will fail.
                    .add(PROVISIONER_START, EXECUTE_FAILURE)
                    .add(PROVISIONER_STOP).addAll(executesCommand())
                    .add(PROVISIONER_DESTROY).addAll(executesCommand())
                    .build()},

            new Object[] {4, EXECUTE, SUCCEEDS, TestRun.State.SETTING_UP,
                ImmutableList.builder()
                    .add(PROVISIONER_GET_RESOURCE_REQUIREMENTS)
                    .add(PROVISIONER_RESERVE).addAll(executesCommand())
                    .add(PROVISIONER_CREATE).addAll(executesCommand())
                    .add(PROVISIONER_PREPARE).addAll(executesCommand())
                    // The abort happens _during_ the command execution in
                    // the PROVISIONER_START call, so the command is killed
                    .add(PROVISIONER_START).addAll(killsCommand())
                    .add(PROVISIONER_STOP).addAll(executesCommand())
                    .add(PROVISIONER_DESTROY).addAll(executesCommand())
                    .build()},

            new Object[] {1, MODULE_RUN, SUCCEEDS, TestRun.State.RUNNING,
                ImmutableList.builder()
                    .add(PROVISIONER_GET_RESOURCE_REQUIREMENTS)
                    .add(PROVISIONER_RESERVE).addAll(executesCommand())
                    .add(PROVISIONER_CREATE).addAll(executesCommand())
                    .add(PROVISIONER_PREPARE).addAll(executesCommand())
                    .add(PROVISIONER_START).addAll(executesCommand())
                    .add(MODULE_RUN, EXECUTE_FAILURE, EXECUTE_FAILURE)
                    .add(PROVISIONER_STOP).addAll(executesCommand())
                    .add(PROVISIONER_DESTROY).addAll(executesCommand())
                    .build()},

            new Object[] {5, EXECUTE, SUCCEEDS, TestRun.State.RUNNING,
                ImmutableList.builder()
                    .add(PROVISIONER_GET_RESOURCE_REQUIREMENTS)
                    .add(PROVISIONER_RESERVE).addAll(executesCommand())
                    .add(PROVISIONER_CREATE).addAll(executesCommand())
                    .add(PROVISIONER_PREPARE).addAll(executesCommand())
                    .add(PROVISIONER_START).addAll(executesCommand())
                    .add(MODULE_RUN).addAll(killsCommand()).add(EXECUTE_FAILURE)
                    .add(PROVISIONER_STOP).addAll(executesCommand())
                    .add(PROVISIONER_DESTROY).addAll(executesCommand())
                    .build()},

            new Object[] {1, PROVISIONER_STOP, SUCCEEDS, TestRun.State.TEARING_DOWN,
                ImmutableList.builder()
                    .add(PROVISIONER_GET_RESOURCE_REQUIREMENTS)
                    .add(PROVISIONER_RESERVE).addAll(executesCommand())
                    .add(PROVISIONER_CREATE).addAll(executesCommand())
                    .add(PROVISIONER_PREPARE).addAll(executesCommand())
                    .add(PROVISIONER_START).addAll(executesCommand())
                    .add(MODULE_RUN).addAll(executesCommand()).addAll(executesCommand())
                    .add(MODULE_RUN).addAll(executesCommand()).addAll(executesCommand())
                    .add(PROVISIONER_STOP).addAll(executesCommand())
                    .add(PROVISIONER_DESTROY).addAll(executesCommand())
                    .build()},

            new Object[] {9, EXECUTE, SUCCEEDS, TestRun.State.TEARING_DOWN,
                ImmutableList.builder()
                    .add(PROVISIONER_GET_RESOURCE_REQUIREMENTS)
                    .add(PROVISIONER_RESERVE).addAll(executesCommand())
                    .add(PROVISIONER_CREATE).addAll(executesCommand())
                    .add(PROVISIONER_PREPARE).addAll(executesCommand())
                    .add(PROVISIONER_START).addAll(executesCommand())
                    .add(MODULE_RUN).addAll(executesCommand()).addAll(executesCommand())
                    .add(MODULE_RUN).addAll(executesCommand()).addAll(executesCommand())
                    .add(PROVISIONER_STOP).addAll(executesCommand())
                    .add(PROVISIONER_DESTROY).addAll(executesCommand())
                    .build()},

            new Object[] {10, EXECUTE, SUCCEEDS, TestRun.State.TEARING_DOWN,
                ImmutableList.builder()
                    .add(PROVISIONER_GET_RESOURCE_REQUIREMENTS)
                    .add(PROVISIONER_RESERVE).addAll(executesCommand())
                    .add(PROVISIONER_CREATE).addAll(executesCommand())
                    .add(PROVISIONER_PREPARE).addAll(executesCommand())
                    .add(PROVISIONER_START).addAll(executesCommand())
                    .add(MODULE_RUN).addAll(executesCommand()).addAll(executesCommand())
                    .add(MODULE_RUN).addAll(executesCommand()).addAll(executesCommand())
                    .add(PROVISIONER_STOP).addAll(executesCommand())
                    .add(PROVISIONER_DESTROY).addAll(executesCommand())
                    .build()},
        };
    }

    public int abortDuringMethodNumber;
    public Method abortDuringMethod;
    public MethodResult methodResult;

    @ParameterizedTest(name = "when the test is aborted during call {0} to {1} that {2}, " +
        "then the test fails during {3}, and only {4} are called")
    @MethodSource("params")
    public void aborts_test_during(int abortDuringMethodNumber, Method abortDuringMethod, MethodResult methodResult,
        TestRun.State expectedFailedDuring, List<Method> expectedMethods)
    {
        this.abortDuringMethodNumber = abortDuringMethodNumber;
        this.abortDuringMethod = abortDuringMethod;
        this.methodResult = methodResult;

        abortsTestDuring(abortDuringMethod, expectedFailedDuring, expectedMethods);
    }
}
