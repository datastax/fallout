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

import org.junit.Before;
import org.junit.Test;

import com.datastax.fallout.TestHelpers;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for Property Specs/Groups/etc
 */
public class PropertyTest extends TestHelpers.FalloutTest
{
    private WritablePropertyGroup pg;

    @Before
    public void setUp()
    {
        pg = new WritablePropertyGroup();
    }

    private <T> void assertValidates(PropertySpec<T> p, String expectedPropName, T expectedValue)
    {
        assertThat(p.validate(pg)).hasValue(expectedPropName);
        assertThat(p.value(pg)).isEqualTo(expectedValue);
    }

    private void assertValidatesWithNoValue(PropertySpec<?> p)
    {
        assertThat(p.validate(pg)).isEmpty();
        assertThat(p.optionalValue(pg)).isEmpty();
    }

    private <T> void assertValidatesWithDefaultValue(PropertySpec<?> p, T expectedDefaultValue)
    {
        assertThat(p.validate(pg)).isEmpty();
        assertThat(p.value(pg)).isEqualTo(expectedDefaultValue);
    }

    private void assertDoesNotValidate(PropertySpec<?>... specs)
    {
        assertThatThrownBy(() -> pg.validateFull(Arrays.asList(specs)))
            .isInstanceOf(PropertySpec.ValidationException.class);
    }

    @Test
    public void simple_property()
    {
        PropertySpec<String> p = PropertySpecBuilder
            .createStr("animals.", "[a-z]+")
            .name("cat")
            .category("animal")
            .required()
            .build();

        pg.put("animals.cat", "calico");

        assertValidates(p, "animals.cat", "calico");

        pg.put("animals.cat", 2333);

        assertThatThrownBy(() -> p.validate(pg))
            .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void options_work_with_direct_values()
    {
        PropertySpec<String> p = PropertySpecBuilder
            .create("animals.")
            .name("cat")
            .category("animal")
            .options(PropertySpec.Value.of("calico", "type1"), PropertySpec.Value.of("siamese"))
            .required()
            .build();

        pg.put("animals.cat", "calico");

        assertValidates(p, "animals.cat", "calico");

        assertThat(p.value(pg)).isEqualTo("calico");
        assertThat(p.valueType(pg).category).hasValue("type1");
    }

    @Test
    public void options_do_not_allow_unspecified_values()
    {
        PropertySpec<String> p = PropertySpecBuilder
            .createStr("animals.")
            .name("cat")
            .options("bagpuss", "buxton")
            .required()
            .build();

        pg.put("animals.cat", "custard");

        assertDoesNotValidate(p);

        pg.put("animals.cat", "buxton");

        assertValidates(p, "animals.cat", "buxton");
    }

    enum Dogs
    {
        RHUBARB, SHEP, ALISTAIR
    };

    @Test
    public void options_work_with_enums()
    {
        PropertySpec<Dogs> p = PropertySpecBuilder
            .<Dogs>create("animals.")
            .name("dog")
            .optionsArray(Dogs.values())
            .required()
            .build();

        pg.put("animals.dog", "goldie");

        assertDoesNotValidate(p);

        pg.put("animals.dog", "shep");

        assertValidates(p, "animals.dog", Dogs.SHEP);
    }

    @Test
    public void suggestions_allow_unspecified_values()
    {
        PropertySpec<String> p = PropertySpecBuilder
            .createStr("animals.")
            .name("cat")
            .suggestions("bagpuss", "buxton")
            .required()
            .build();

        pg.put("animals.cat", "custard");

        assertValidates(p, "animals.cat", "custard");

        pg.put("animals.cat", "buxton");

        assertValidates(p, "animals.cat", "buxton");
    }

    @Test
    public void validate_on_non_requires()
    {
        PropertySpec<String> p = PropertySpecBuilder
            .createStr("animals.", "[0-9]+")
            .name("cat")
            .category("animal")
            .build();

        PropertySpec<String> p2 = PropertySpecBuilder
            .createStr("animals.")
            .name("dog")
            .category("animal")
            .required()
            .build();

        assertDoesNotValidate(p2);

        //this should pass because it's optional
        assertValidatesWithNoValue(p);
    }

    @Test
    public void deprecated_names()
    {
        String cassandraInstallType = "cassandra.install.type";
        String cassandraGitBranch = "cassandra.git.branch";
        String prefix = "config_manager.";

        PropertySpec<String> productInstallType = PropertySpecBuilder
            .createStr(prefix, "[a-z]+")
            .name("product.install.type")
            .deprecatedName(cassandraInstallType)
            .required()
            .build();

        PropertySpec<String> productVersion = PropertySpecBuilder.createStr(prefix)
            .name("product.version")
            .deprecatedName(cassandraGitBranch)
            .description(
                "The version of the product. Can be a git branch/tag/commit or a tarball/package version number.")
            .defaultOf("trunk")
            .build();

        pg.put(prefix + cassandraInstallType, "git");
        pg.put(prefix + cassandraGitBranch, "5.0.0");

        productInstallType.validate(pg);
        productVersion.validate(pg);

        assertThat(productInstallType.value(pg)).isEqualTo("git");
        assertThat(productVersion.value(pg)).isEqualTo("5.0.0");
    }

    @Test
    public void validate_returns_used_property_name()
    {
        PropertySpec<String> p = PropertySpecBuilder
            .createStr("test.")
            .name("name")
            .alias("oldtest.alias") // need to include prefix
            .deprecatedName("deprecated")
            .build();

        assertThat(p.validate(pg)).isEmpty();

        pg.put("test.deprecated", "val");
        assertThat(p.validate(pg)).hasValue("test.deprecated");

        pg.put("oldtest.alias", "val");
        assertThat(p.validate(pg)).hasValue("oldtest.alias");

        pg.put("test.name", "val");
        assertThat(p.validate(pg)).hasValue("test.name");
    }

    @Test
    public void validate_finds_duplicate_names()
    {
        PropertySpec<String> p1 = PropertySpecBuilder
            .createStr("test.")
            .name("name1")
            .build();

        PropertySpec<String> p2 = PropertySpecBuilder
            .createStr("test.")
            .name("name1")
            .build();

        assertDoesNotValidate(p1, p2);
    }

    @Test
    public void validate_finds_duplicate_names_and_alias()
    {
        PropertySpec<String> p1 = PropertySpecBuilder
            .createStr("test.")
            .name("name1")
            .build();

        PropertySpec<String> p2 = PropertySpecBuilder
            .createStr("test.")
            .name("name2")
            .alias("test.name1")
            .build();

        assertDoesNotValidate(p1, p2);
    }

    @Test
    public void validate_finds_duplicate_name_and_deprecated_name()
    {
        PropertySpec<String> p1 = PropertySpecBuilder
            .createStr("test.")
            .name("name1")
            .build();

        PropertySpec<String> p2 = PropertySpecBuilder
            .createStr("test.")
            .name("name2")
            .deprecatedName("name1")
            .build();

        assertDoesNotValidate(p1, p2);
    }

    @Test
    public void validate_finds_unused_properties()
    {
        PropertySpec<String> p1 = PropertySpecBuilder
            .createStr("test.")
            .name("name1")
            .build();

        PropertySpec<String> p2 = PropertySpecBuilder
            .createStr("test.")
            .name("name2")
            .build();

        pg.put("test.name1", "val");
        pg.put("test.foobar", "val");

        assertThatCode(() -> pg.validate(Arrays.asList(p1, p2), false))
            .doesNotThrowAnyException();

        assertDoesNotValidate(p1, p2);
    }

    @Test
    public void default_string_values_are_used()
    {
        final PropertySpec<String> p = PropertySpecBuilder.createStr("test.")
            .name("colours")
            .suggestions("red", "pink", "chartreuse")
            .defaultOf("mauve")
            .build();

        assertValidatesWithDefaultValue(p, "mauve");
    }

    @Test
    public void default_enum_values_are_used()
    {
        final PropertySpec<Dogs> p = PropertySpecBuilder.<Dogs>create("test.")
            .name("dogs")
            .optionsArray(Dogs.values())
            .defaultOf(Dogs.SHEP)
            .build();

        assertValidatesWithDefaultValue(p, Dogs.SHEP);
    }
}
