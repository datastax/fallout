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
package com.datastax.fallout.service.resources.server;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.base.Functions;
import io.dropwizard.auth.Auth;

import com.datastax.fallout.harness.ArtifactChecker;
import com.datastax.fallout.harness.Checker;
import com.datastax.fallout.harness.Module;
import com.datastax.fallout.ops.ConfigurationManager;
import com.datastax.fallout.ops.FalloutPropertySpecs;
import com.datastax.fallout.ops.PropertyBasedComponent;
import com.datastax.fallout.ops.PropertySpec;
import com.datastax.fallout.ops.Provisioner;
import com.datastax.fallout.service.FalloutConfiguration;
import com.datastax.fallout.service.core.User;
import com.datastax.fallout.service.views.FalloutView;
import com.datastax.fallout.service.views.MainView;
import com.datastax.fallout.util.component_discovery.ComponentFactory;

@Path("/components/{type: (provisioners|configurationmanagers|modules|checkers|artifact_checkers)}/{name}")
@Produces(MediaType.TEXT_HTML)
public class ComponentResource
{
    public static class PropertyCategory
    {
        final String category;
        final List<PropertySpec<?>> properties;

        PropertyCategory(String category, List<PropertySpec<?>> properties)
        {
            this.category = category;
            this.properties = properties;
        }

        boolean isDependencyCategory()
        {
            return category.contains("=");
        }
    }

    public static class ComponentProperties
    {
        final PropertyBasedComponent component;
        final Collection<PropertyCategory> propertyCategories;

        ComponentProperties(PropertyBasedComponent component)
        {
            this.component = component;
            this.propertyCategories = propertyCategories(component.getPropertySpecs());
        }
    }

    public static class ComponentType
    {
        final String type;
        final String name;
        final String description;
        final List<ComponentProperties> components;
        private final Map<String, ComponentProperties> componentLookup;

        ComponentType(String type, String name, String description, List<ComponentProperties> components)
        {
            this.type = type;
            this.name = name;
            this.description = description;
            this.components = components;
            componentLookup = components.stream()
                .collect(Collectors.toMap(
                    componentProperties -> componentProperties.component.name(), Functions.identity()));
        }
    }

    private final List<ComponentType> componentTypes;
    private final Map<String, ComponentType> componentTypeLookup;
    private MainView mainView;

    public static Collection<PropertyCategory> propertyCategories(List<PropertySpec<?>> specs)
    {
        final var categorySpecsMap = new HashMap<String, List<PropertySpec<?>>>();

        specs.stream()

            //Filter out any properties with system aliases we use on this page
            .filter(spec -> spec.alias()
                .map(alias -> !alias.startsWith(FalloutPropertySpecs.prefix))
                .orElse(true))

            //Filter out any properties with the internal category
            .filter(spec -> !spec.isInternal())

            .forEach(spec -> categorySpecsMap.computeIfAbsent(spec.category().orElse(""),
                ignored -> new ArrayList<>()).add(spec));

        return categorySpecsMap
            .entrySet()
            .stream()
            .map(categorySpecs -> new PropertyCategory(categorySpecs.getKey(),
                categorySpecs.getValue()))
            .sorted(Comparator
                .<PropertyCategory>comparingInt(propertyCategory -> propertyCategory.isDependencyCategory() ? 1 : 0)
                .thenComparing(propertyCategory -> propertyCategory.category))
            .toList();
    }

    private static List<ComponentProperties> loadComponents(FalloutConfiguration configuration,
        ComponentFactory componentFactory, Class<? extends PropertyBasedComponent> clazz)
    {
        return componentFactory.exampleComponents(clazz)
            .stream()
            .map(c -> {
                if (ConfigurationManager.class.isAssignableFrom(c.getClass()))
                {
                    ((ConfigurationManager) c).setFalloutConfiguration(configuration);
                }
                return new ComponentProperties(c);
            })
            .sorted(Comparator.comparing(
                componentProperties -> componentProperties.component.name().toLowerCase()))
            .toList();
    }

    private static ComponentType loadComponentType(FalloutConfiguration configuration,
        ComponentFactory componentFactory, String type)
    {
        switch (type)
        {
            case "provisioners":
                return new ComponentType(type, "Provisioners",
                    "Supplies compute / disk / network resources for a test.",
                    loadComponents(configuration, componentFactory, Provisioner.class));
            case "configurationmanagers":
                return new ComponentType(type, "Configuration Managers",
                    "Installs and configures software and services.",
                    loadComponents(configuration, componentFactory, ConfigurationManager.class));
            case "modules":
                return new ComponentType(type, "Modules", "Executes actions during a test.",
                    loadComponents(configuration, componentFactory, Module.class));
            case "checkers":
                return new ComponentType(type, "Checkers",
                    "Verifies information contained in the jepsen log once a test completes.",
                    loadComponents(configuration, componentFactory, Checker.class));
            case "artifact_checkers":
                return new ComponentType(type, "Artifact Checkers",
                    "Checks / Extracts information from one or more downloaded artifacts once a test completes.",
                    loadComponents(configuration, componentFactory, ArtifactChecker.class));
        }

        return null;
    }

    public ComponentResource(FalloutConfiguration configuration, ComponentFactory componentFactory)
    {
        this.componentTypes = Stream
            .of("provisioners", "configurationmanagers", "modules", "checkers", "artifact_checkers")
            .map(type -> loadComponentType(configuration, componentFactory, type))
            .toList();
        this.componentTypeLookup = componentTypes.stream()
            .collect(Collectors.toMap(componentType -> componentType.type, Functions.identity()));
    }

    public void setMainView(MainView mainView)
    {
        this.mainView = mainView;
    }

    public List<ComponentType> getComponentTypes()
    {
        return componentTypes;
    }

    class ComponentDocsView extends FalloutView
    {
        final ComponentType componentType;
        final ComponentProperties componentProperties;

        public ComponentDocsView(Optional<User> user, ComponentType componentType, String name)
        {
            super(List.of(name, "Docs"), "component-docs.mustache", user, mainView);
            this.componentType = componentType;
            this.componentProperties = componentType != null ? componentType.componentLookup.get(name) : null;
        }
    }

    @GET
    public FalloutView getDocumentation(@Auth Optional<User> user, @PathParam("type") String type,
        @PathParam("name") String name)
    {
        return new ComponentDocsView(user, componentTypeLookup.get(type), name);
    }
}
