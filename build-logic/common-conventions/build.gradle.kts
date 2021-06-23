buildscript {
    configurations.classpath {
        resolutionStrategy.activateDependencyLocking()
    }
}

plugins {
    `kotlin-dsl`

    id("org.jlleitschuh.gradle.ktlint") version "10.1.0"
}

group = "com.datastax"

// We can't define a variable, ktlintVersion, and use it in the plugins
// block, so instead we extract the version used in the plugins block.
fun getPluginVersion(prefix: String) =
    buildscript.configurations["classpath"].dependencies
        .find {
            it.name.startsWith(prefix)
        }!!.version

dependencies {
    api("org.jlleitschuh.gradle:ktlint-gradle:${getPluginVersion("org.jlleitschuh.gradle.ktlint")}")
}

// see https://docs.gradle.org/current/userguide/kotlin_dsl.html#sec:kotlin-dsl_plugin
kotlinDslPluginOptions {
    experimentalWarning.set(false)
}

dependencyLocking {
    lockAllConfigurations()
}

tasks.register("lint") {
    dependsOn("ktlintCheck")
}
