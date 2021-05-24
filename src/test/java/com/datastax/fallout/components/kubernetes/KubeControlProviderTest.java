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
package com.datastax.fallout.components.kubernetes;

import java.util.Optional;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import static com.datastax.fallout.assertj.Assertions.assertThat;
import static com.datastax.fallout.components.kubernetes.KubeControlProvider.kubectl;

class KubeControlProviderTest
{
    @ParameterizedTest
    @CsvSource({
        "the-namespace,log stuff,kubectl --namespace=the-namespace log stuff",
        ",log stuff,kubectl log stuff",
        "the-namespace,kots install sentry-pro,kubectl kots --namespace=the-namespace install sentry-pro",
        ",kots install sentry-pro,kubectl kots install sentry-pro"
    })
    void inserts_namespace_when_a_namespace_is_provided(String namespace, String command, String expected)
    {
        assertThat(kubectl(Optional.ofNullable(namespace), command))
            .isEqualTo(expected);
    }
}
