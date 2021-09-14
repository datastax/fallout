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
package com.datastax.fallout.ops;

import java.util.Map;

import com.datastax.fallout.exceptions.InvalidConfigurationException;

public class UserSecretsPropertyRefHandler extends PropertyRefExpander.Handler
{
    private final Map<String, String> userSecrets;

    public UserSecretsPropertyRefHandler(Map<String, String> userSecrets)
    {
        super("secret");
        this.userSecrets = userSecrets;
    }

    @Override
    public String expandKey(String secretKey)
    {
        String secret = userSecrets.get(secretKey);
        if (secret == null)
        {
            throw new InvalidConfigurationException(String.format("%s is not in the user credentials", secretKey));
        }
        return secret;
    }
}
