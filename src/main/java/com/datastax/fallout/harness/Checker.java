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
package com.datastax.fallout.harness;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import clojure.lang.APersistentMap;
import clojure.lang.Keyword;
import clojure.lang.PersistentArrayMap;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.fallout.ops.Ensemble;
import com.datastax.fallout.ops.PropertyGroup;
import com.datastax.fallout.ops.WritablePropertyGroup;

import static com.datastax.fallout.harness.ClojureApi.get;
import static com.datastax.fallout.harness.JepsenApi.ENSEMBLE;
import static com.datastax.fallout.harness.JepsenApi.VALID;

/**
 * Checkers determine the validity of a test run. Checkers run after the completion of all
 * phases of modules. When creating a checker, this class should be extended.
 *
 * Checkers will run as part of Jepsen by implementing the check method of the
 * jepsen.checker.Checker interface. this class provides more pleasant interoperability by
 * extracting the ensemble and history from the test map before invoking the
 * check(ensemble, history) method of this abstract class.
 *
 * The check method takes a Collection so it is not necessarily the case that the history
 * will be chronologically sorted. Each Operation in the history has a time field that can be
 * used for sorting if necessary.
 *
 * @see Operation
 */
public abstract class Checker implements jepsen.checker.Checker, WorkloadComponent
{
    private static final Logger classLogger = LoggerFactory.getLogger(Checker.class);

    private String instanceName;
    private PropertyGroup checkerInstanceProperties;
    protected Logger logger = classLogger;

    public void setLogger(Logger logger)
    {
        this.logger = logger;
    }

    protected Logger logger()
    {
        return logger;
    }

    @Override
    public void setProperties(PropertyGroup properties)
    {
        Preconditions.checkArgument(checkerInstanceProperties == null, "checker instance properties already set");
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
        Preconditions.checkArgument(this.instanceName == null, "checker instance name already set");
        this.instanceName = instanceName;
    }

    @Override
    public String getInstanceName()
    {
        return instanceName;
    }

    @Override
    public Object check(Object test, Object model, Object history)
    {
        Ensemble ensemble = (Ensemble) get.invoke(test, ENSEMBLE);
        Map<Keyword, Object> resultsMap = new HashMap<>();
        List<Operation> historyList = ((Collection<APersistentMap>) history).stream()
            .map(Operation::fromOpMap)
            .collect(Collectors.toList());

        boolean checkResult = false;
        try
        {
            checkResult = check(ensemble, historyList);
        }
        catch (Throwable e)
        {
            logger.error("Checker threw an exception", e);
        }

        if (checkResult)
        {
            logger.info("Checker '{}' has passed the check.", getInstanceName());
        }
        else
        {
            logger.error("Checker '{}' has failed the check.", getInstanceName());
        }

        resultsMap.put(VALID, checkResult);
        return PersistentArrayMap.create(resultsMap);
    }

    public abstract boolean check(Ensemble ensemble, Collection<Operation> history);
}
