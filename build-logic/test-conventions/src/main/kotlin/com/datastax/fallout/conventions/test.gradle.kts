package com.datastax.fallout.conventions

import org.gradle.api.tasks.testing.Test
import org.gradle.api.tasks.testing.logging.TestExceptionFormat
import org.gradle.api.tasks.testing.logging.TestLogEvent

plugins {
    java
}

dependencies {
    testImplementation(platform("org.junit:junit-bom:5.+"))
    testImplementation("org.junit.jupiter:junit-jupiter-api")
    testImplementation("org.junit.jupiter:junit-jupiter-params")
    testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine")

    testImplementation("org.assertj:assertj-core:3.+")
}

project.afterEvaluate {
    tasks.withType(Test::class.java) {
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
