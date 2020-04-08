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
package com.datastax.fallout.service.core;

import com.google.common.collect.ImmutableSet;

import static org.assertj.core.api.Assertions.assertThat;

public class DeletedTestTest
{
    @org.junit.Test
    public void test_runs_and_deleted_tests_are_equal()
    {
        Test test = Test.createTest("moonunit@example.com", "dweezil", "nope");
        test.setTags(ImmutableSet.of("flaky", "best"));

        DeletedTest deletedTest = DeletedTest.fromTest(test);
        assertThat(deletedTest.getOwner()).isEqualTo(test.getOwner());
        assertThat(deletedTest.getName()).isEqualTo(test.getName());
        assertThat(deletedTest.getTestId()).isEqualTo(test.getTestId());
        assertThat(deletedTest.getCreatedAt()).isEqualTo(test.getCreatedAt());
        assertThat(deletedTest.getDefinition()).isEqualTo(test.getDefinition());
        assertThat(deletedTest.getTags()).isEqualTo(test.getTags());
        assertThat(deletedTest.getSizeOnDiskBytes()).isEqualTo(test.getSizeOnDiskBytes());
    }

}
