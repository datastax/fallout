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
package com.datastax.fallout.service.resources.server;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Lists;
import io.dropwizard.auth.Auth;

import com.datastax.fallout.harness.ArtifactChecker;
import com.datastax.fallout.harness.Checker;
import com.datastax.fallout.harness.Module;
import com.datastax.fallout.ops.ConfigurationManager;
import com.datastax.fallout.ops.PropertyBasedComponent;
import com.datastax.fallout.ops.Provisioner;
import com.datastax.fallout.ops.Utils;
import com.datastax.fallout.service.FalloutConfiguration;
import com.datastax.fallout.service.core.User;
import com.datastax.fallout.service.resources.server.TestResource.ComponentView.ComponentCategories;
import com.datastax.fallout.service.views.FalloutView;
import com.datastax.fallout.service.views.MainView;

@Path("/components/{type: (provisioners|configurationmanagers|modules|checkers|artifact_checkers)}")
@Produces(MediaType.TEXT_HTML)
public class ComponentResource
{
    private final FalloutConfiguration configuration;

    private final LoadingCache<String, ComponentType> componentTypeInfoCache =
        CacheBuilder.newBuilder().build(new CacheLoader<String, ComponentType>()
        {
            public ComponentType load(String key) throws Exception
            {
                ComponentType componentType = null;

                switch (key)
                {
                    case "provisioners":
                        componentType = new ComponentType(key, "Provisioners",
                            "Supplies compute / disk / network resources for a test.",
                            getCategoryInfo(Provisioner.class));
                        break;
                    case "configurationmanagers":
                        componentType = new ComponentType(key, "Configuration Managers",
                            "Installs and configures software and services.",
                            getCategoryInfo(ConfigurationManager.class));
                        break;
                    case "modules":
                        componentType = new ComponentType(key, "Modules", "Executes actions during a test.",
                            getCategoryInfo(Module.class));
                        break;
                    case "checkers":
                        componentType = new ComponentType(key, "Checkers",
                            "Verifies information contained in the jepsen log once a test completes.",
                            getCategoryInfo(Checker.class));
                        break;
                    case "artifact_checkers":
                        componentType = new ComponentType(key, "Artifact Checkers",
                            "Checks / Extracts information from one or more downloaded artifacts once a test completes.",
                            getCategoryInfo(ArtifactChecker.class));
                        break;
                }

                return componentType;
            }
        });

    private final List<ComponentType> componentMenu;
    private MainView mainView;

    public ComponentResource(FalloutConfiguration configuration)
    {
        this.configuration = configuration;
        List<String> componentTypes = Lists
            .newArrayList("provisioners", "configurationmanagers", "modules", "checkers", "artifact_checkers");
        componentMenu = componentTypes.stream()
            .map(componentTypeInfoCache::getUnchecked)
            .collect(Collectors.toList());
    }

    public List<ComponentType> getComponentMenu()
    {
        return componentMenu;
    }

    @GET
    public FalloutView getDocumentation(@Auth Optional<User> user, @PathParam("type") String type)
    {
        return new ComponentDocsView(user, componentTypeInfoCache.getUnchecked(type));
    }

    public void setMainView(MainView mainView)
    {
        this.mainView = mainView;
    }

    public static class ComponentType
    {
        final String key;
        final String name;
        final String blurb;
        final List<ComponentCategories> categories;

        ComponentType(String key, String name, String blurb, List<ComponentCategories> categories)
        {
            this.key = key;
            this.name = name;
            this.blurb = blurb;
            categories.sort(Comparator.comparing(ComponentCategories::getName));
            this.categories = categories;
        }
    }

    List<ComponentCategories> getCategoryInfo(Class<? extends PropertyBasedComponent> clazz)
    {
        return Utils.loadComponents(clazz)
            .stream()
            .map(c -> {
                if (ConfigurationManager.class.isAssignableFrom(c.getClass()))
                {
                    ((ConfigurationManager) c).setFalloutConfiguration(configuration);
                }
                return new ComponentCategories(c);
            })
            .collect(Collectors.toList());
    }

    class ComponentDocsView extends FalloutView
    {
        final ComponentType componentType;

        public ComponentDocsView(Optional<User> user, ComponentType componentType)
        {
            super("component-docs.mustache", user, mainView);
            this.componentType = componentType;
        }
    }
}
