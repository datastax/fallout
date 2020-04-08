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
package com.datastax.fallout.ops;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.base.Preconditions;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

import com.datastax.fallout.service.resources.server.TestResource;
import com.datastax.fallout.util.Duration;

import static com.datastax.fallout.util.YamlUtils.dumpYaml;

/**
 * General builder for PropertySpecs.
 *
 * Not required but useful for most things.
 *
 * @see PropertySpec
 *
 */
public class PropertySpecBuilder<T>
{
    private static final Logger logger = LoggerFactory.getLogger(PropertySpecBuilder.class);

    public static PropertySpecBuilder<Boolean> createBool(String prefix, boolean defaultValue)
    {
        PropertySpecBuilder<Boolean> res = create(prefix);
        res.options(true, false);
        res.defaultOf(defaultValue);
        return res;
    }

    public static PropertySpecBuilder<Integer> createInt(String prefix)
    {
        PropertySpecBuilder<Integer> res = create(prefix);
        res.parser(s -> Integer.valueOf(s.toString()));
        res.validator("^[0-9]+$");
        return res;
    }

    public static PropertySpecBuilder<Long> createLong(String prefix)
    {
        PropertySpecBuilder<Long> res = create(prefix);
        res.parser(s -> Long.valueOf(s.toString()));
        res.validator("^[0-9]+$");
        return res;
    }

    public static PropertySpecBuilder<String> createStr(String prefix)
    {
        return createStrBase(prefix).validator(PropertySpecBuilder::validateNonEmptyString);
    }

    public static PropertySpecBuilder<String> createStr(String prefix, String validationRegex)
    {
        return createStrBase(prefix).validator(validationRegex);
    }

    public static PropertySpecBuilder<String> createStr(String prefix, Function<Object, Boolean> validationMethod)
    {
        return createStrBase(prefix).validator(validationMethod);
    }

    private static PropertySpecBuilder<String> createStrBase(String prefix)
    {
        PropertySpecBuilder<String> res = create(prefix);
        res.parser(Object::toString);
        return res;
    }

    public static PropertySpecBuilder<String> createName(String prefix)
    {
        return PropertySpecBuilder.<String>create(prefix).asNameProperty();
    }

    public static PropertySpecBuilder<Long> createIterations(String prefix)
    {
        PropertySpecBuilder<Long> res = create(prefix);
        res.parser(s -> Utils.parseLong(s.toString()));
        res.validator("[0-9]+[kKmMbB]?");
        return res;
    }

    public static PropertySpecBuilder<List<String>> createStrList(String prefix)
    {
        PropertySpecBuilder<List<String>> res = create(prefix);
        res.parser(s -> (List<String>) s);
        res.validator(input -> input instanceof List &&
            ((List) input).stream().allMatch(PropertySpecBuilder::validateNonEmptyString));
        return res;
    }

    public static PropertySpecBuilder<Duration> createDuration(String prefix)
    {
        PropertySpecBuilder<Duration> res = create(prefix);
        res.parser(s -> Duration.fromString(s.toString()));
        res.dumper(Duration::toString);
        res.validator(Duration.DURATION_RE);
        return res;
    }

    public static PropertySpecBuilder<Pattern> createRegex(String prefix)
    {
        PropertySpecBuilder<Pattern> res = create(prefix);
        res.parser(s -> Pattern.compile(s.toString()));
        res.dumper(Pattern::toString);
        return res;
    }

    public static PropertySpecBuilder<List<Pattern>> createRegexList(String prefix)
    {
        PropertySpecBuilder<List<Pattern>> res = create(prefix);
        res.parser(l -> ((List<String>) l).stream().map(Pattern::compile).collect(Collectors.toList()));
        res.dumper(l -> l.stream().map(Pattern::toString).collect(Collectors.joining(" ")));
        return res;
    }

    public static boolean validateNonEmptyString(Object input)
    {
        return input instanceof String && !input.toString().trim().isEmpty();
    }

    public static <T> PropertySpecBuilder<T> create(String prefix)
    {
        return new PropertySpecBuilder<>(prefix);
    }

    public static PropertySpec<String> serverGroup(String prefix)
    {
        return nodeGroup(prefix, "server_group", "The server to operate against", "server");
    }

    public static PropertySpec<String> clientGroup(String prefix)
    {
        return nodeGroup(prefix, "client_group", "The client to run on", "client");
    }

    public static PropertySpec<String> nodeGroup(String prefix)
    {
        return nodeGroup(prefix, "node_group", "The node group to operate against", null);
    }

    public static PropertySpec<String> nodeGroup(String prefix, String name, String description, String defaultVal)
    {
        PropertySpecBuilder<String> res = PropertySpecBuilder
            .createStr(prefix)
            .name(name)
            .description(description);
        if (defaultVal != null)
        {
            res = res.defaultOf(defaultVal);
        }
        return res.build();
    }

    private String prefix;
    private String shortName;
    private String name;
    private PropertySpec.Value<T> defaultValue;
    private Collection<PropertySpec.Value<T>> valueOptions;
    private boolean valueOptionsMustMatch;
    private Pattern validationRegex;
    private Function<Object, Boolean> validationMethod;
    private Function<Object, T> valueMethod;
    private Function<T, String> dumperMethod;
    private String description;
    private String category;
    private PropertySpec dependsOn;
    private String alias;
    private Boolean required;
    private String deprecatedName;
    private String deprecatedShortName;

    private PropertySpecBuilder(String prefix)
    {
        this.prefix = prefix;
    }

    /**
     * Sets the default value to be used if none is specified
     * @param defaultValue
     * @return
     */
    public PropertySpecBuilder<T> defaultOf(PropertySpec.Value<T> defaultValue)
    {
        Preconditions.checkArgument(this.defaultValue == null, "default value already set");
        this.defaultValue = defaultValue;

        return this;
    }

    public PropertySpecBuilder<T> defaultOf(T defaultValue)
    {
        return defaultOf(PropertySpec.Value.of(defaultValue));
    }

    /**
     * Sets the possible values of this property.
     * @param options the possible values, must not be null nor empty
     * @return the current instance of {@code PropertySpecBuilder}
     */
    @SafeVarargs
    public final PropertySpecBuilder<T> options(T... options)
    {
        return this.options(Arrays.asList(options));
    }

    public final PropertySpecBuilder<T> optionsArray(T[] options)
    {
        return this.options(Arrays.asList(options));
    }

    public PropertySpecBuilder<T> options(Collection<?> options)
    {
        return options(true, options);
    }

    @SafeVarargs
    public final PropertySpecBuilder<T> suggestions(T... options)
    {
        return this.suggestions(Arrays.asList(options));
    }

    public final PropertySpecBuilder<T> suggestionsArray(T[] options)
    {
        return this.suggestions(Arrays.asList(options));
    }

    public PropertySpecBuilder<T> suggestions(Collection<?> options)
    {
        return options(false, options);
    }

    /**
     * Sets the possible values for
     * @param options
     * @return
     */
    private PropertySpecBuilder<T> options(boolean mustMatch, Collection<?> options)
    {
        Preconditions.checkArgument(this.valueOptions == null, "value options already set");
        Preconditions.checkArgument(options != null && !options.isEmpty(), "at least one value option must be set");
        this.valueOptions = options.stream()
            .map(option -> option instanceof PropertySpec.Value ? (PropertySpec.Value<T>) option :
                PropertySpec.Value.of((T) option))
            .collect(Collectors.toList());
        this.valueOptionsMustMatch = mustMatch;
        return this;
    }

    public PropertySpecBuilder<T> required()
    {
        return required(true);
    }

    /**
     * Specified if the property is required (default false)
     * @param required
     * @return
     */
    public PropertySpecBuilder<T> required(boolean required)
    {
        Preconditions.checkArgument(this.required == null, "required flag already set");
        this.required = required;

        return this;
    }

    @SuppressWarnings("unchecked")
    public PropertySpecBuilder<String> asNameProperty()
    {
        return ((PropertySpecBuilder<String>) this)
            .validator("^" + TestResource.NAME_PATTERN + "$")
            .parser(Object::toString);
    }

    /**
     * Applies the regex to the supplied value to validate it.
     * This only works for simple String properties.
     *
     * Regex patten is matched with Matcher.find() so for non repeating
     * inputs please use ^ and $ to encompass the entire input for validation
     *
     * @see Matcher#find()
     *
     * @param regex
     * @return
     */
    public PropertySpecBuilder<T> validator(String regex)
    {
        return validator(Pattern.compile(regex));
    }

    /**
     * Applies the pattern to the supplied value to validate it.
     * This only works for simple String properties.
     *
     * Regex patten is matched with Matcher.find() so for non repeating
     * inputs please use ^ and $ to encompass the entire input for validation
     *
     * @see Matcher#find()
     *
     * @param regex
     * @return
     */
    public PropertySpecBuilder<T> validator(Pattern pattern)
    {
        Preconditions.checkArgument(validationMethod == null, "validation method already set");

        validationRegex = pattern;

        validationMethod = (input) -> {
            Matcher m = pattern.matcher(input.toString());
            return m.find();
        };

        return this;
    }

    /**
     * Supply custom method to validator the property value.
     * @param validationMethod
     * @return
     */
    public PropertySpecBuilder<T> validator(Function<Object, Boolean> validationMethod)
    {
        Preconditions.checkArgument(this.validationMethod == null, "validation method already set");

        this.validationMethod = validationMethod;

        return this;
    }

    /**
     * Name of the property
     * @param name
     * @return
     */
    public PropertySpecBuilder<T> name(String name)
    {
        Preconditions.checkArgument(this.name == null, "property name already set");
        this.shortName = name;
        this.name = prefix + name;

        return this;
    }

    /**
     * A long description of the property
     * @param desc
     * @return
     */
    public PropertySpecBuilder<T> description(String desc)
    {
        Preconditions.checkArgument(description == null, "property description already set");
        this.description = desc;

        return this;
    }

    public PropertySpecBuilder<T> internal()
    {
        return this.category("internal");
    }

    /**
     * Label similar properties with the same category
     */
    public PropertySpecBuilder<T> category(String category)
    {
        Preconditions.checkArgument(this.category == null, "category already set");
        this.category = category;

        return this;
    }

    private PropertySpecBuilder<T> dependsOn(PropertySpec otherProperty)
    {
        Preconditions.checkArgument(this.dependsOn == null, "dependsOn already set");
        this.dependsOn = otherProperty;

        return this;
    }

    /**
     * Links one property as the child of another
     *
     * @param otherProperty
     * @return
     */
    public <O> PropertySpecBuilder<T> dependsOn(PropertySpec<O> otherProperty, O otherPropertyValue)
    {
        PropertySpec.Value<O> otherPropVal = PropertySpec.Value.of(otherPropertyValue);
        if (!otherPropVal.category.isPresent())
        {
            throw new IllegalStateException(
                "PropertySpec.Value.category missing for property value: " + otherPropertyValue);
        }
        this.category(otherPropVal.category.get());
        return this.dependsOn(otherProperty);
    }

    /**
     * Creates an alias name for this property (note: this alias must include a prefix already!)
     * @param alias
     * @return
     */
    public PropertySpecBuilder<T> alias(String alias)
    {
        Preconditions.checkArgument(this.alias == null, "alias already set");
        this.alias = alias;

        return this;
    }

    /**
     * Creates a deprecatedName alias for this property for backward compatibility.
     * @param deprecatedName
     * @return
     */
    public PropertySpecBuilder<T> deprecatedName(String deprecatedName)
    {
        Preconditions.checkArgument(this.deprecatedName == null, "deprecatedName already set");
        this.deprecatedShortName = deprecatedName;
        this.deprecatedName = prefix + deprecatedName;

        return this;
    }

    /**
     * Method for extracting the Value type from the property value
     * For example, parse string to get a double: Double.valueOf("1.2");
     * @param valueMethod
     * @return
     */
    public PropertySpecBuilder<T> parser(Function<Object, T> valueMethod)
    {
        Preconditions.checkArgument(this.valueMethod == null, "value method already set");
        this.valueMethod = valueMethod;

        return this;
    }

    /** Set a method for converting a value to a valid YAML input; if
     * unset, then whatever {@link Yaml#dump} produces will be used */
    public PropertySpecBuilder<T> dumper(Function<T, String> dumperMethod)
    {
        Preconditions.checkArgument(this.dumperMethod == null, "dumper method already set");
        this.dumperMethod = dumperMethod;

        return this;
    }

    /**
     * Internal method to validator the builder properties
     */
    private void check()
    {
        Preconditions.checkArgument(name != null, "Property name missing");
        Preconditions.checkArgument(valueOptions != null || valueMethod != null, "Value method missing");
        Preconditions.checkArgument(
            !(this.validationRegex != null && this.valueOptions != null && this.valueOptionsMustMatch),
            "Cant have regexp validator and matching options at the same time");

        if (required == null)
        {
            /*
            If a property has a default value, the common case is that it's required.
            However, we need to allow for redundant calls of required():  defaultOf(x).required();
            and for unusual cases where a property has a default value but it's optional: defaultOf(x).required(false).
            */
            required = this.defaultValue != null;
        }

        if (description == null)
            description = "Property name: " + name + ", required = " + required;

        if (valueOptions != null && defaultValue != null)
        {
            for (PropertySpec.Value v : valueOptions)
            {
                if (v.value.equals(defaultValue.value))
                    v.isDefault = true;
            }
        }

        if (dependsOn != null)
        {
            if (category == null)
                throw new IllegalArgumentException("category required when dependsOn is set " + name);

            if (!dependsOn.isOptionsOnly())
                throw new IllegalArgumentException(
                    "Invalid dependsOn propertySpec (must be optionsOnly) " + dependsOn.name());
        }
    }

    /**
     * Generates the property spec instance
     * @return
     */
    public <T1 extends T> PropertySpec<T1> build()
    {
        try
        {
            check();
        }
        catch (Throwable t)
        {
            // this helps debugging when components cannot be instantiated
            logger.error("PropertySpecBuilder.check failed for property: " + name, t);
            throw t;
        }

        return new PropertySpec<T1>()
        {
            @Override
            public Optional<String> deprecatedName()
            {
                return Optional.ofNullable(deprecatedName);
            }

            @Override
            public Optional<String> deprecatedShortName()
            {
                return Optional.ofNullable(deprecatedShortName);
            }

            @Override
            public String name()
            {
                return name;
            }

            @Override
            public String shortName()
            {
                return shortName;
            }

            @Override
            public String prefix()
            {
                return prefix;
            }

            @Override
            public Optional<String> alias()
            {
                return Optional.ofNullable(alias);
            }

            @Override
            public String describe()
            {
                return description;
            }

            @Override
            public Optional<String> category()
            {
                return Optional.ofNullable(category);
            }

            @Override
            public Optional<PropertySpec> dependsOn()
            {
                return Optional.ofNullable(dependsOn);
            }

            @Override
            public Optional<Value<T1>> defaultValue()
            {
                return Optional.ofNullable((Value<T1>) defaultValue);
            }

            @Override
            public Optional<String> defaultValueYaml()
            {
                return defaultValue().map(value -> dumperMethod != null ? dumperMethod.apply(value.value) :
                    dumpYaml(value.value).replaceFirst("^!![^ ]* ", ""));
            }

            @Override
            public Optional<String> validationPattern()
            {
                return Optional.ofNullable(validationRegex).map(Pattern::toString);
            }

            @Override
            public T1 value(PropertyGroup propertyGroup)
            {
                return valueType(propertyGroup).value;
            }

            @Override
            public Optional<T1> optionalValue(PropertyGroup propertyGroup)
            {
                return Optional.ofNullable(valueType(propertyGroup).value);
            }

            private Pair<Optional<String>, Value<T1>> validatedValueType(PropertyGroup propertyGroup)
            {
                final Optional<Pair<String, Object>> foundPropNameAndValue = Stream
                    .of(name, alias, deprecatedName)
                    .map(propName -> Pair.of(propName, propertyGroup.get(propName)))
                    .filter(pair -> pair.getRight() != null)
                    .findFirst();

                final Optional<String> usedPropName = foundPropNameAndValue.map(Pair::getLeft);
                final Object value = foundPropNameAndValue.map(Pair::getRight).orElse(null);

                if (value == null)
                {
                    if (defaultValue != null)
                    {

                        /* Note that we don't validate defaultValue: this is because for a PropertySpec<T>, default
                         * values are specified as the type of the final value, T.  Validation however is specified
                         * as Function<Object, Boolean>, where the input is of whatever type was passed in: this
                         * means that attempting to change the behaviour so that we validate the default value
                         * would mean converting T to whatever came in (which may not be a string). */
                        return Pair.of(usedPropName, (Value<T1>) defaultValue);
                    }
                    if (isRequired(propertyGroup))
                    {
                        throw new ValidationException(this, "Missing required property");
                    }
                    // No need to validate null
                    return Pair.of(usedPropName, (Value<T1>) Value.empty);
                }

                if (valueOptions != null && (value instanceof String || value instanceof Boolean))
                {
                    final String strVal = value.toString();

                    final Optional<Value<T>> matchingOption = valueOptions.stream()
                        .filter(option -> option.id.equalsIgnoreCase(strVal))
                        .findFirst();

                    if (matchingOption.isPresent())
                    {
                        return Pair.of(usedPropName, (Value<T1>) matchingOption.get());
                    }

                    if (valueOptionsMustMatch)
                    {
                        List<String> optionIds = valueOptions.stream().map(v -> v.id).collect(Collectors.toList());
                        throw new ValidationException(this,
                            String.format("Given value \"%s\" is not available in options: %s", strVal, optionIds));
                    }
                }

                if (valueMethod == null)
                {
                    throw new ValidationException(this,
                        String.format(
                            "Value method is null with value '%s' - are you passing in an invalid choice for an option spec?",
                            value));
                }

                //Use validation method
                if (validationMethod != null && !validationMethod.apply(value))
                {
                    if (validationRegex != null)
                        throw new ValidationException(this,
                            String.format("Regexp \"%s\" failed for value: \"%s\"", validationRegex, value));
                    else
                        throw new ValidationException(this,
                            String.format("validationMethod failed for value: \"%s\"", value));
                }

                return Pair.of(usedPropName, Value.of((T1) valueMethod.apply(value), category));
            }

            @Override
            public Value<T1> valueType(PropertyGroup propertyGroup)
            {
                return validatedValueType(propertyGroup).getRight();
            }

            @Override
            public boolean isRequired()
            {
                return required && defaultValue == null;
            }

            @Override
            public boolean isRequired(PropertyGroup propertyGroup)
            {
                return isRequired() &&
                    //if the parent choice isn't selected then mark this not required
                    (dependsOn == null || dependsOn.valueType(propertyGroup).category.get().equals(category));
            }

            @Override
            public boolean isOptionsOnly()
            {
                return valueMethod == null && valueOptions != null && valueOptionsMustMatch;
            }

            @Override
            public Optional<Collection<Value<T1>>> options()
            {
                return Optional.ofNullable((Collection) valueOptions);
            }

            @Override
            public Optional<String> validate(PropertyGroup propertyGroup) throws ValidationException
            {
                return validatedValueType(propertyGroup).getLeft();
            }

            @Override
            public String toString()
            {
                return "DefaultPropertySpec(" + this.name() + ")";
            }
        };
    }
}
