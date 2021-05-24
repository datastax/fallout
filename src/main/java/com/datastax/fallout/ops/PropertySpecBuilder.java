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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.base.Preconditions;
import com.google.common.collect.Range;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

import com.datastax.fallout.components.common.provider.FileProvider;
import com.datastax.fallout.components.kubernetes.HelmChartConfigurationManager;
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

    /** Creates a builder for a spec that will perform a checked cast of the input to clazz, followed by an
     * unchecked cast to T.  This is hopefully a pragmatic halfway house: if we <i>really</i> want to validate,
     * say, that a <code>Map&lt;String, Integer&gt;</code> actually does have <code>String</code> keys and
     * <code>Integer</code> values then we should probably fix the code that parses the YAML (using Jackson
     * and {@link com.fasterxml.jackson.core.type.TypeReference}), rather than validating after the fact.
     */
    @SuppressWarnings("unchecked")
    private static <S, T extends S> PropertySpecBuilder<T> createRaw(String prefix, Class<S> clazz)
    {
        return PropertySpecBuilder.<T>create(prefix)
            .parser(obj -> (T) clazz.cast(obj));
    }

    private static PropertySpecBuilder<Boolean> createBool(String prefix, Optional<Boolean> defaultValue)
    {
        PropertySpecBuilder<Boolean> res = createRaw(prefix, Boolean.class);
        res.options(true, false);
        defaultValue.ifPresent(res::defaultOf);
        return res;
    }

    public static PropertySpecBuilder<Boolean> createBool(String prefix, boolean defaultValue)
    {
        return createBool(prefix, Optional.of(defaultValue));
    }

    public static PropertySpecBuilder<Boolean> createBool(String prefix)
    {
        return createBool(prefix, Optional.empty());
    }

    public static <E extends Enum<E>> PropertySpecBuilder<E> createEnum(String prefix, Class<E> enumClass)
    {
        final var builder = PropertySpecBuilder.<E>create(prefix)
            .parser(createEnumParser(enumClass));
        builder.enumValues = enumClass.getEnumConstants();
        return builder;
    }

    public static <T> PropertySpecBuilder<Map<String, T>> createMap(String prefix)
    {
        return PropertySpecBuilder.createRaw(prefix, Map.class);
    }

    private static <T extends Number> PropertySpecBuilder<T> createNumber(
        String prefix, Class<T> numberClass, Function<String, T> fromString)
    {
        return PropertySpecBuilder.<T>create(prefix)
            .parser(input -> numberClass.isInstance(input) ?
                numberClass.cast(input) :
                fromString.apply(Objects.toString(input)));
    }

    private static <T extends Number & Comparable<T>> PropertySpecBuilder<T> createNumber(
        String prefix, Class<T> numberClass, Function<String, T> fromString, Range<T> validRange)
    {
        return createNumber(prefix, numberClass, fromString)
            .validator(validRange::contains);
    }

    public static PropertySpecBuilder<Integer> createInt(String prefix)
    {
        return createNumber(prefix, Integer.class, Integer::valueOf);
    }

    public static PropertySpecBuilder<Integer> createInt(String prefix, Range<Integer> validRange)
    {
        return createNumber(prefix, Integer.class, Integer::valueOf, validRange);
    }

    public static PropertySpecBuilder<Long> createLong(String prefix)
    {
        return createNumber(prefix, Long.class, Long::valueOf);
    }

    public static PropertySpecBuilder<Double> createDouble(String prefix)
    {
        return createNumber(prefix, Double.class, Double::valueOf);
    }

    public static PropertySpecBuilder<Double> createDouble(String prefix, Range<Double> validRange)
    {
        return createNumber(prefix, Double.class, Double::valueOf, validRange);
    }

    /** Parse any object as a string using {@link Objects#toString} */
    public static PropertySpecBuilder<String> createStr(String prefix)
    {
        return createStrBase(prefix).validator(PropertySpecBuilder::validateNonEmptyString);
    }

    /** Require that the input is actually a string */
    public static PropertySpecBuilder<String> createStrictStr(String prefix)
    {
        return createRaw(prefix, String.class).validator(PropertySpecBuilder::validateNonEmptyString);
    }

    public static PropertySpecBuilder<String> createStr(String prefix, String validationRegex)
    {
        return createStrBase(prefix).validator(Pattern.compile(validationRegex));
    }

    public static PropertySpecBuilder<String> createStr(String prefix, Pattern validationPattern)
    {
        return createStrBase(prefix).validator(validationPattern);
    }

    public static PropertySpecBuilder<String> createStr(String prefix, Predicate<String> validationMethod)
    {
        return createStrBase(prefix).validator(validationMethod);
    }

    private static PropertySpecBuilder<String> createStrBase(String prefix)
    {
        return PropertySpecBuilder.<String>create(prefix).parser(Object::toString);
    }

    public static PropertySpecBuilder<FileProvider.LocalManagedFileRef> createLocalManagedFileRef(String prefix)
    {
        PropertySpecBuilder<FileProvider.LocalManagedFileRef> res = create(prefix);
        res.parser(o -> new FileProvider.LocalManagedFileRef(o.toString()));
        res.expandRefsMode = PropertyGroup.ExpandRefsMode.IGNORE_REFS;
        return res;
    }

    public static PropertySpecBuilder<FileProvider.RemoteManagedFileRef> createRemoteManagedFileRef(String prefix)
    {
        PropertySpecBuilder<FileProvider.RemoteManagedFileRef> res = create(prefix);
        res.parser(o -> new FileProvider.RemoteManagedFileRef(o.toString()));
        res.expandRefsMode = PropertyGroup.ExpandRefsMode.IGNORE_REFS;
        return res;
    }

    public static PropertySpecBuilder<List<FileProvider.RemoteManagedFileRef>>
        createRemoteManagedFileRefList(String prefix)
    {
        PropertySpecBuilder<List<FileProvider.RemoteManagedFileRef>> res = create(prefix);
        res.parser(o -> ((List<Object>) o).stream()
            .map(ref -> new FileProvider.RemoteManagedFileRef(ref.toString()))
            .collect(Collectors.toList()));
        res.expandRefsMode = PropertyGroup.ExpandRefsMode.IGNORE_REFS;
        return res;
    }

    public static PropertySpecBuilder<String> createName(String prefix)
    {
        return PropertySpecBuilder.createStrBase(prefix).asNameProperty();
    }

    public static PropertySpecBuilder<Long> createIterations(String prefix)
    {
        PropertySpecBuilder<Long> res = create(prefix);
        res.parser(s -> Utils.parseLong(s.toString()));
        return res;
    }

    public static PropertySpecBuilder<List<String>> createStrList(String prefix)
    {
        PropertySpecBuilder<List<String>> res = createRaw(prefix, List.class);
        res.validator(input -> input.stream().allMatch(PropertySpecBuilder::validateNonEmptyString));
        return res;
    }

    public static PropertySpecBuilder<Duration> createDuration(String prefix)
    {
        PropertySpecBuilder<Duration> res = create(prefix);
        res.parser(s -> Duration.fromString(s.toString()));
        res.dumper(Duration::toString);
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

    public static boolean validateNonEmptyString(String input)
    {
        return !input.trim().isEmpty();
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
        return nodeGroup(prefix, null);
    }

    public static PropertySpec<String> nodeGroup(String prefix, String defaultVal)
    {
        return nodeGroup(prefix, "node_group", "The node group to operate against", defaultVal);
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

    private static class Dependency<T>
    {
        private final PropertySpec<T> propertySpec;
        private final T value;

        Dependency(PropertySpec<T> propertySpec, T value)
        {
            this.propertySpec = propertySpec;
            this.value = value;
        }

        boolean isSatisfied(PropertyGroup properties)
        {
            return Objects.equals(propertySpec.value(properties), value);
        }
    }

    private Supplier<String> prefix;
    private String shortName;
    private Supplier<String> name;
    private T defaultValue;
    private Collection<T> valueOptions;
    private boolean valueOptionsMustMatch;
    private T[] enumValues;
    private Pattern validationRegex;
    private Predicate<T> validationMethod;
    private Function<Object, T> valueMethod;
    private Function<T, String> dumperMethod;
    private String description;
    private String category;
    private boolean isInternal;
    private Dependency<?> dependsOn;
    private String alias;
    private Boolean required;
    private String deprecatedName;
    private String deprecatedShortName;
    private PropertyGroup.ExpandRefsMode expandRefsMode = PropertyGroup.ExpandRefsMode.EXPAND_REFS;

    private PropertySpecBuilder(String prefix)
    {
        this.prefix = () -> prefix;
    }

    /**
     * Change a prefix at runtime to allow for the same component to be used many times
     * in the same nodegroup, See {@link HelmChartConfigurationManager}
     */
    public PropertySpecBuilder<T> runtimePrefix(Supplier<String> prefix)
    {
        Preconditions.checkArgument(prefix != null, "prefix can not be null");
        this.prefix = prefix;

        return this;
    }

    public PropertySpecBuilder<T> defaultOf(T defaultValue)
    {
        Preconditions.checkArgument(this.defaultValue == null, "default value already set");
        this.defaultValue = defaultValue;

        return this;
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

    public PropertySpecBuilder<T> options(Collection<T> options)
    {
        return options(true, options);
    }

    @SafeVarargs
    public final PropertySpecBuilder<T> suggestions(T... options)
    {
        return this.suggestions(Arrays.asList(options));
    }

    public PropertySpecBuilder<T> suggestions(Collection<T> options)
    {
        return options(false, options);
    }

    /**
     * Sets the possible values for
     * @param options
     * @return
     */
    private PropertySpecBuilder<T> options(boolean mustMatch, Collection<T> options)
    {
        Preconditions.checkArgument(this.valueOptions == null, "value options already set");
        Preconditions.checkArgument(options != null && !options.isEmpty(), "at least one value option must be set");
        this.valueOptions = new ArrayList<>(options);
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

    public PropertySpecBuilder<String> asNameProperty()
    {
        return validator(Pattern.compile(TestResource.NAME_PATTERN));
    }

    /**
     * Applies the pattern to the supplied value to validate it.
     * This only works for simple String properties.
     *
     * Regex patten is matched with {@link Matcher#matches()} i.e. the whole string
     * must be matched by the pattern.
     */
    private PropertySpecBuilder<String> validator(Pattern pattern)
    {
        Preconditions.checkArgument(validationMethod == null, "validation method already set");

        validationRegex = pattern;

        validationMethod = (input) -> {
            Matcher m = pattern.matcher(input.toString());
            return m.matches();
        };

        return (PropertySpecBuilder<String>) this;
    }

    /**
     * Supply custom method to validator the property value.
     * @param validationMethod
     * @return
     */
    public PropertySpecBuilder<T> validator(Predicate<T> validationMethod)
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
        this.name = () -> prefix.get() + name;

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
        isInternal = true;
        return this;
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

    /**
     * Links one property as the child of another
     *
     * @param otherProperty
     * @return
     */
    public <O> PropertySpecBuilder<T> dependsOn(PropertySpec<O> otherProperty, O otherPropertyValue)
    {
        Preconditions.checkNotNull(otherPropertyValue);
        Preconditions.checkArgument(this.dependsOn == null, "dependsOn already set");
        Preconditions.checkArgument(otherProperty.isOptionsOnly(),
            "Invalid dependsOn propertySpec (must be optionsOnly)");

        this.dependsOn = new Dependency<>(otherProperty, otherPropertyValue);
        this.category(String.format("%s = %s", otherProperty.shortName(), otherPropertyValue));

        return this;
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
        this.deprecatedName = prefix.get() + deprecatedName;

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

    /** Do not use this for new properties; it's here to support old special-cased properties that could reference
     *  remote file _or_ be used with a plain URL. */
    @Deprecated
    public PropertySpecBuilder<T> disableRefExpansion()
    {
        expandRefsMode = PropertyGroup.ExpandRefsMode.IGNORE_REFS;
        return this;
    }

    /** Create a parser for enums that performs caseless comparison on the values */
    @SuppressWarnings("unchecked")
    private static <E extends Enum<E>> Function<Object, E> createEnumParser(Class<?> enumType)
    {
        final Class<E> e = (Class<E>) enumType;
        final var enumValues = Arrays.stream(e.getEnumConstants())
            .collect(Collectors.toMap(value -> String.valueOf(value).toLowerCase(Locale.ROOT), value -> value));
        return input -> {
            final var inputStr = String.valueOf(input);
            return Optional.ofNullable(enumValues.get(inputStr.toLowerCase(Locale.ROOT)))
                .orElseThrow(() -> new RuntimeException(String.format(
                    "Given value \"%s\" is not available in options: %s", inputStr, enumValues.keySet())));
        };
    }

    /**
     * Internal method to validate the builder properties
     */
    private void check()
    {
        Preconditions.checkArgument(name != null, "Property name missing");
        Preconditions.checkArgument(valueMethod != null, "Value method missing");
        Preconditions.checkArgument(
            !(this.validationRegex != null && this.valueOptions != null && this.valueOptionsMustMatch),
            "Cant have regexp validator and matching options at the same time");

        if (description == null)
        {
            description = "Property name: " + name.get() + ", required = " + required;
        }
    }

    /**
     * Generates the property spec instance
     * @return
     */
    public PropertySpec<T> build()
    {
        try
        {
            check();
        }
        catch (Throwable t)
        {
            // this helps debugging when components cannot be instantiated
            logger.error("PropertySpecBuilder.check failed for property: " + name.get(), t);
            throw t;
        }

        return new PropertySpec<T>() {
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
                return name.get();
            }

            @Override
            public String shortName()
            {
                return shortName;
            }

            @Override
            public String prefix()
            {
                return prefix.get();
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
            public boolean isInternal()
            {
                return isInternal;
            }

            @Override
            public Optional<T> defaultValue()
            {
                return Optional.ofNullable(defaultValue);
            }

            @Override
            public Optional<String> defaultValueYaml()
            {
                return defaultValue().map(value -> dumperMethod != null ? dumperMethod.apply(value) :
                    dumpYaml(value).replaceFirst("^!![^ ]* ", ""));
            }

            @Override
            public Optional<String> validationPattern()
            {
                return Optional.ofNullable(validationRegex).map(Pattern::toString);
            }

            @Override
            public T value(PropertyGroup propertyGroup)
            {
                return validatedValueType(propertyGroup).getRight();
            }

            @Override
            public Optional<T> optionalValue(PropertyGroup propertyGroup)
            {
                return Optional.ofNullable(value(propertyGroup));
            }

            private Pair<Optional<String>, T> validatedValueType(PropertyGroup propertyGroup)
            {
                final Optional<Pair<String, Object>> foundPropNameAndValue = Stream
                    .of(name.get(), alias, deprecatedName)
                    .filter(Objects::nonNull)
                    .map(propName -> Pair.of(propName, propertyGroup.get(propName, expandRefsMode)))
                    .filter(pair -> pair.getRight() != null)
                    .findFirst();

                final Optional<String> usedPropName = foundPropNameAndValue.map(Pair::getLeft);
                final Object rawValue = foundPropNameAndValue.map(Pair::getRight).orElse(null);

                if (rawValue == null)
                {
                    if (defaultValue != null)
                    {
                        /* Note that we don't validate defaultValue: this is because for a PropertySpec<T>, default
                         * values are specified as the type of the final rawValue, T.  Validation however is specified
                         * as Function<Object, Boolean>, where the input is of whatever type was passed in: this
                         * means that attempting to change the behaviour so that we validate the default rawValue
                         * would mean converting T to whatever came in (which may not be a string). */
                        return Pair.of(usedPropName, defaultValue);
                    }
                    if (isRequired(propertyGroup))
                    {
                        throw new ValidationException(this, "Missing required property");
                    }
                    // No need to validate null
                    return Pair.of(usedPropName, null);
                }

                final T value;
                try
                {
                    value = valueMethod.apply(rawValue);
                }
                catch (Throwable e)
                {
                    throw new ValidationException(this,
                        String.format("Could not parse input \"%s\": %s", rawValue, e.getMessage()));
                }

                if (value == null)
                {
                    throw new ValidationException(this,
                        String.format("Parser returned null for input \"%s\"", rawValue));
                }

                if (validationMethod != null && !validationMethod.test(value))
                {
                    if (validationRegex != null)
                    {
                        throw new ValidationException(this,
                            String.format("Regexp \"%s\" failed for value: \"%s\"", validationRegex, rawValue));
                    }
                    else
                    {
                        throw new ValidationException(this,
                            String.format("validationMethod failed for value: \"%s\" (parsed value \"%s\")",
                                rawValue, value));
                    }
                }

                if (valueOptions != null)
                {
                    final Optional<T> matchingOption = valueOptions.stream()
                        .filter(option -> Objects.equals(option, value))
                        .findFirst();

                    if (matchingOption.isPresent())
                    {
                        return Pair.of(usedPropName, matchingOption.get());
                    }

                    if (valueOptionsMustMatch)
                    {
                        List<String> optionStrings = valueOptions.stream()
                            .map(String::valueOf).collect(Collectors.toList());
                        throw new ValidationException(this,
                            String.format("Given value \"%s\" is not available in options: %s", value, optionStrings));
                    }
                }

                return Pair.of(usedPropName, value);
            }

            @Override
            public boolean isRequired()
            {
                return required != null && required;
            }

            @Override
            public boolean isRequired(PropertyGroup propertyGroup)
            {
                return isRequired() &&
                    //if the parent choice isn't selected then mark this not required
                    (dependsOn == null || dependsOn.isSatisfied(propertyGroup));
            }

            @Override
            public boolean isOptionsOnly()
            {
                return enumValues != null || (valueOptions != null && valueOptionsMustMatch);
            }

            @Override
            public Optional<Collection<T>> options()
            {
                if (valueOptions != null)
                {
                    return Optional.ofNullable(valueOptions);
                }
                else if (enumValues != null)
                {
                    return Optional.of(Arrays.asList(enumValues));
                }
                else
                {
                    return Optional.empty();
                }
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
