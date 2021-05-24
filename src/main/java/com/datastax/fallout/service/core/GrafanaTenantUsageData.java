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
package com.datastax.fallout.service.core;

/**
 * A container class which holds urls pointing to the Grafana panels detailing its current usage.
 * These Urls are parsed and displayed in current-tests.mustache
 */
public class GrafanaTenantUsageData
{
    private String name;

    private String baseUrl;

    private String instanceUrl;

    private String coresUrl;

    private String ramUrl;

    public String getName()
    {
        return name;
    }

    public String getBaseUrl()
    {
        return baseUrl;
    }

    public String getInstanceUrl()
    {
        return instanceUrl;
    }

    public String getCoresUrl()
    {
        return coresUrl;
    }

    public String getRamUrl()
    {
        return ramUrl;
    }
}
