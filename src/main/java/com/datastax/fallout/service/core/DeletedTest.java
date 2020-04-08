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

import com.fasterxml.jackson.databind.ObjectMapper;

import com.datastax.driver.mapping.annotations.Table;
import com.datastax.fallout.util.Exceptions;

@Table(name = "deleted_tests")
public class DeletedTest extends Test
{
    public static DeletedTest fromTest(Test test)
    {
        return Exceptions.getUnchecked(() -> {
            ObjectMapper mapper = new ObjectMapper();
            byte[] json = mapper.writeValueAsBytes(test);
            return mapper.readValue(json, DeletedTest.class);
        });
    }

}
