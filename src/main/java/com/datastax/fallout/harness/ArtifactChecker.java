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

import java.nio.file.Path;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.fallout.ops.Ensemble;
import com.datastax.fallout.ops.PropertyGroup;
import com.datastax.fallout.ops.WritablePropertyGroup;

/***
 * An ArtifactChecker determines the validity of a test run by examining artifacts.
 * <p/>
 * ArtifactCheckers run after the ensemble and the nodegroup got destroyed and only if artifacts were downloaded.
 */
public abstract class ArtifactChecker implements WorkloadComponent
{
    private static final Logger classLogger = LoggerFactory.getLogger(ArtifactChecker.class);

    private String instanceName;
    private PropertyGroup checkerInstanceProperties;
    private Logger logger = classLogger;

    @Override
    public void setProperties(PropertyGroup properties)
    {
        Preconditions.checkArgument(checkerInstanceProperties == null,
            "artifact checker instance properties already set");
        checkerInstanceProperties = properties;
    }

    @Override
    public PropertyGroup getProperties()
    {
        return checkerInstanceProperties != null ? checkerInstanceProperties : new WritablePropertyGroup();
    }

    @Override
    public void setInstanceName(String instanceName)
    {
        Preconditions.checkArgument(this.instanceName == null, "artifact checker instance name already set");
        this.instanceName = instanceName;
    }

    @Override
    public String getInstanceName()
    {
        return instanceName;
    }

    public void setLogger(Logger logger)
    {
        this.logger = logger;
    }

    protected Logger logger()
    {
        return logger;
    }

    /**
     * @param ensemble             The {@link Ensemble} instance
     * @param rootArtifactLocation A {@link Path} describing the root location of all artifacts
     * @return <code>true</code> if validation was successful, <code>false</code> otherwise.
     */
    public abstract boolean checkArtifacts(Ensemble ensemble, Path rootArtifactLocation);
}
