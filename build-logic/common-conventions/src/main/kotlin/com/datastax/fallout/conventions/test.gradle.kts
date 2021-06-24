package com.datastax.fallout.conventions

import org.gradle.api.internal.tasks.testing.junitplatform.JUnitPlatformTestFramework
import org.gradle.api.tasks.testing.logging.TestExceptionFormat
import org.gradle.api.tasks.testing.logging.TestLogEvent

plugins {
    java
}

// --------------------------------------------------------------------------------------------------------------------
// Common dependencies for testing

// Create configurations to hold common test dependencies.  Rather than directly adding dependencies to the
// standard testImplementation/testRuntimeOnly dependencies, this allows us to declare other test configurations
// (such as implementation test) in projects using this plugin and have those configurations `extendFrom` these.

val testConventionsImplementationDeps by configurations.creating {
    isCanBeResolved = false
}

val testConventionsRuntimeOnlyDeps by configurations.creating {
    isCanBeResolved = false
}

configurations {
    testImplementation.get().extendsFrom(testConventionsImplementationDeps)
    testRuntimeOnly.get().extendsFrom(testConventionsRuntimeOnlyDeps)
}

dependencies {
    testConventionsImplementationDeps(platform("org.junit:junit-bom:5.+"))
    testConventionsImplementationDeps("org.junit.jupiter:junit-jupiter-api")
    testConventionsImplementationDeps("org.junit.jupiter:junit-jupiter-params")

    testConventionsRuntimeOnlyDeps(platform("org.junit:junit-bom:5.+"))
    testConventionsRuntimeOnlyDeps("org.junit.jupiter:junit-jupiter-engine")

    testConventionsImplementationDeps("org.assertj:assertj-core:3.+")
}

// --------------------------------------------------------------------------------------------------------------------
// Common settings for testing

project.afterEvaluate {
    tasks.withType(Test::class.java) {

        // Calling useJUnitPlatform overwrites any existing options, so only do it
        // if we're not already using it
        if (testFramework !is JUnitPlatformTestFramework) {
            useJUnitPlatform()
        }

        testLogging {
            events = setOf(TestLogEvent.SKIPPED, TestLogEvent.FAILED, TestLogEvent.PASSED)
            exceptionFormat = TestExceptionFormat.FULL
        }

        this.let { testTask ->
            reports {
                html.isEnabled = false
                junitXml.isEnabled = true
                junitXml.isOutputPerTestCase = true
                junitXml.destination = file("$buildDir/reports/junit/${testTask.name}")
            }
        }

        if (rootProject.hasProperty("testIgnoreFailures")) {
            ignoreFailures = project.findProperty("testIgnoreFailures") != null
        }

        // Make check tasks depend on this
        tasks.getByPath("cleanCheck").dependsOn("clean${name.capitalize()}")
        tasks.getByPath("check").dependsOn(this)

        // Disable output caching: we don't want to cache the output of tests
        outputs.cacheIf { false }
    }
}
