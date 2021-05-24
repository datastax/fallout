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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;

import com.datastax.fallout.runner.CheckResourcesResult;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.willThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.quality.Strictness.STRICT_STUBS;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = STRICT_STUBS)
public class ActiveTestRunTest
{
    @Mock
    ActiveTestRun activeTestRun;

    @Mock
    ExceptionHandler logger;

    @AfterEach
    public void theActiveTestRunIsAlwaysClosed()
    {
        verify(activeTestRun).close();
    }

    private void whenActiveTestRunIsRun()
    {
        ActiveTestRun.run(activeTestRun, logger);
    }

    private void thenTheEnsembleIsSetup()
    {
        verify(activeTestRun, times(1)).setup();
    }

    private void thenTheEnsembleIsNotSetup()
    {
        verify(activeTestRun, never()).setup();
    }

    private void thenTheWorkloadIsRun()
    {
        verify(activeTestRun, times(1)).runWorkload();
    }

    private void thenTheWorkloadIsNotRun()
    {
        verify(activeTestRun, never()).runWorkload();
    }

    private void thenArtifactsAreDownloaded()
    {
        verify(activeTestRun, times(1)).downloadArtifacts();
    }

    private void thenArtifactsAreNotDownloaded()
    {
        verify(activeTestRun, never()).downloadArtifacts();
    }

    private void thenTheEnsembleIsTornDown()
    {
        verify(activeTestRun, times(1)).tearDownEnsemble();
    }

    private void thenTheEnsembleIsNotTornDown()
    {
        verify(activeTestRun, never()).tearDownEnsemble();
    }

    private void thenArtifactsAreChecked()
    {
        verify(activeTestRun, times(1)).checkArtifacts();
    }

    private void thenArtifactsAreNotChecked()
    {
        verify(activeTestRun, never()).checkArtifacts();
    }

    private void thenTheActiveTestRunIsFailed()
    {
        verify(activeTestRun, times(1)).failTest(any(), any());
        verify(activeTestRun, never()).failTestTemporarily(any());
    }

    private void thenTheActiveTestRunIsFailedTemporarily()
    {
        verify(activeTestRun, never()).failTest(any(), any());
        verify(activeTestRun, times(1)).failTestTemporarily(any());
    }

    private void thenTheActiveTestRunIsNotFailed()
    {
        verify(activeTestRun, never()).failTest(any(), any());
        verify(activeTestRun, never()).failTestTemporarily(any());
    }

    private void thenLastResortHandledExceptionsAre(int loggedExceptions)
    {
        verify(logger, times(loggedExceptions)).accept(any(), any());
    }

    @Test
    public void exceptions_cannot_escape_from_checkResources()
    {
        given(activeTestRun.checkResources()).willThrow(new RuntimeException());

        whenActiveTestRunIsRun();

        thenTheEnsembleIsNotSetup();
        thenTheWorkloadIsNotRun();
        thenArtifactsAreNotDownloaded();
        thenTheEnsembleIsNotTornDown();
        thenArtifactsAreNotChecked();

        thenLastResortHandledExceptionsAre(0);
        thenTheActiveTestRunIsFailed();
    }

    @Test
    public void checkResources_unavailable_is_treated_as_temporary()
    {
        given(activeTestRun.checkResources()).willReturn(CheckResourcesResult.UNAVAILABLE);

        whenActiveTestRunIsRun();

        thenTheEnsembleIsNotSetup();
        thenTheWorkloadIsNotRun();
        thenArtifactsAreNotDownloaded();
        thenTheEnsembleIsNotTornDown();
        thenArtifactsAreNotChecked();

        thenLastResortHandledExceptionsAre(0);
        thenTheActiveTestRunIsFailedTemporarily();
    }

    @Test
    public void checkResources_fail_is_treated_as_fail()
    {
        given(activeTestRun.checkResources()).willReturn(CheckResourcesResult.FAILED);

        whenActiveTestRunIsRun();

        thenTheEnsembleIsNotSetup();
        thenTheWorkloadIsNotRun();
        thenArtifactsAreNotDownloaded();
        thenTheEnsembleIsNotTornDown();
        thenArtifactsAreNotChecked();

        thenLastResortHandledExceptionsAre(0);
        thenTheActiveTestRunIsFailed();
    }

    @Test
    public void setup_unavailable_is_treated_as_temporary()
    {
        given(activeTestRun.checkResources()).willReturn(CheckResourcesResult.AVAILABLE);
        given(activeTestRun.setup()).willReturn(CheckResourcesResult.UNAVAILABLE);

        whenActiveTestRunIsRun();

        thenTheEnsembleIsSetup();
        thenTheWorkloadIsNotRun();
        thenArtifactsAreNotDownloaded();
        thenTheEnsembleIsTornDown();
        thenArtifactsAreNotChecked();

        thenLastResortHandledExceptionsAre(0);
        thenTheActiveTestRunIsFailedTemporarily();
    }

    @Test
    public void setup_fail_is_treated_as_fail()
    {
        given(activeTestRun.checkResources()).willReturn(CheckResourcesResult.AVAILABLE);
        given(activeTestRun.setup()).willReturn(CheckResourcesResult.FAILED);

        whenActiveTestRunIsRun();

        thenTheEnsembleIsSetup();
        thenTheWorkloadIsNotRun();
        thenArtifactsAreDownloaded();
        thenTheEnsembleIsTornDown();
        thenArtifactsAreNotChecked();

        thenLastResortHandledExceptionsAre(0);
        thenTheActiveTestRunIsFailed();
    }

    @Test
    public void exceptions_cannot_escape_from_setup()
    {
        given(activeTestRun.checkResources()).willReturn(CheckResourcesResult.AVAILABLE);
        given(activeTestRun.setup()).willThrow(new RuntimeException());

        whenActiveTestRunIsRun();

        thenTheEnsembleIsSetup();
        thenTheWorkloadIsNotRun();
        thenArtifactsAreDownloaded();
        thenTheEnsembleIsTornDown();
        thenArtifactsAreNotChecked();

        thenLastResortHandledExceptionsAre(0);
        thenTheActiveTestRunIsFailed();
    }

    @Test
    public void exceptions_cannot_escape_from_runWorkload()
    {
        given(activeTestRun.checkResources()).willReturn(CheckResourcesResult.AVAILABLE);
        given(activeTestRun.setup()).willReturn(CheckResourcesResult.AVAILABLE);
        given(activeTestRun.runWorkload()).willThrow(new RuntimeException());

        whenActiveTestRunIsRun();

        thenTheEnsembleIsSetup();
        thenTheWorkloadIsRun();
        thenArtifactsAreDownloaded();
        thenTheEnsembleIsTornDown();
        thenArtifactsAreNotChecked();

        thenLastResortHandledExceptionsAre(0);
        thenTheActiveTestRunIsFailed();
    }

    @Test
    public void exceptions_cannot_escape_from_tearDown()
    {
        given(activeTestRun.checkResources()).willReturn(CheckResourcesResult.AVAILABLE);
        given(activeTestRun.setup()).willReturn(CheckResourcesResult.AVAILABLE);
        given(activeTestRun.runWorkload()).willReturn(true);

        willThrow(new RuntimeException()).given(activeTestRun).tearDownEnsemble();

        whenActiveTestRunIsRun();

        thenTheEnsembleIsSetup();
        thenTheWorkloadIsRun();
        thenArtifactsAreDownloaded();
        thenTheEnsembleIsTornDown();
        thenArtifactsAreChecked();

        thenLastResortHandledExceptionsAre(0);
        thenTheActiveTestRunIsFailed();
    }

    @Test
    public void exceptions_cannot_escape_from_close()
    {
        given(activeTestRun.checkResources()).willReturn(CheckResourcesResult.AVAILABLE);
        given(activeTestRun.setup()).willReturn(CheckResourcesResult.AVAILABLE);
        given(activeTestRun.runWorkload()).willReturn(true);

        willThrow(new RuntimeException()).given(activeTestRun).close();

        whenActiveTestRunIsRun();

        thenTheEnsembleIsSetup();
        thenTheWorkloadIsRun();
        thenArtifactsAreDownloaded();
        thenTheEnsembleIsTornDown();
        thenArtifactsAreChecked();

        thenLastResortHandledExceptionsAre(1);
        // We don't fail the test run on close failures:
        // firstly for semantic reasons, because the test run hasn't actually failed, we just couldn't close it;
        // and secondly for pragmatic reasons, because there's no way to fail it after close has failed.
        thenTheActiveTestRunIsNotFailed();
    }

    @Test
    public void exceptions_cannot_escape_from_failTest()
    {
        given(activeTestRun.checkResources()).willReturn(CheckResourcesResult.AVAILABLE);
        given(activeTestRun.setup()).willReturn(CheckResourcesResult.AVAILABLE);
        given(activeTestRun.runWorkload()).willThrow(new RuntimeException());

        willThrow(new RuntimeException()).given(activeTestRun).failTest(any(), any());

        whenActiveTestRunIsRun();

        thenTheEnsembleIsSetup();
        thenTheWorkloadIsRun();
        thenArtifactsAreDownloaded();
        thenTheEnsembleIsTornDown();
        thenArtifactsAreNotChecked();

        thenLastResortHandledExceptionsAre(1);
        thenTheActiveTestRunIsFailed();
    }
}
