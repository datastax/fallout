buildscript {
    configurations.classpath {
        resolutionStrategy.activateDependencyLocking()
    }
}

plugins {
    `kotlin-dsl`

    id("org.jlleitschuh.gradle.ktlint") version "10.0.0"
}

group = "com.datastax"

// see https://docs.gradle.org/current/userguide/kotlin_dsl.html#sec:kotlin-dsl_plugin
kotlinDslPluginOptions {
    experimentalWarning.set(false)
}

dependencyLocking {
    lockAllConfigurations()
}

configure<org.jlleitschuh.gradle.ktlint.KtlintExtension> {
    version.set("0.41.0")
}

tasks.register("lint") {
    dependsOn("ktlintCheck")
}
