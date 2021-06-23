buildscript {
    configurations.classpath {
        resolutionStrategy.activateDependencyLocking()
    }
}

plugins {
    `kotlin-dsl`

    id("com.datastax.fallout.conventions.test")
    id("org.jlleitschuh.gradle.ktlint") version "10.0.0"
}

group = "com.datastax"

// see https://docs.gradle.org/current/userguide/kotlin_dsl.html#sec:kotlin-dsl_plugin
kotlinDslPluginOptions {
    experimentalWarning.set(false)
}

dependencies {
    implementation("de.undercouch:gradle-download-task:4+")
    implementation("com.google.gradle:osdetector-gradle-plugin:1+")
}

dependencyLocking {
    lockAllConfigurations()
}

gradlePlugin {
    plugins {
        create("symlinksPlugin") {
            id = "com.datastax.fallout.symlinks"
            implementationClass =
                "com.datastax.fallout.gradle.symlinks.SymlinksPlugin"
        }

        create("externalToolsPlugin") {
            id = "com.datastax.fallout.externaltools"
            implementationClass =
                "com.datastax.fallout.gradle.externaltools.ExternalToolsPlugin"
        }
    }
}

tasks.test {
    useJUnitPlatform()
}

configure<org.jlleitschuh.gradle.ktlint.KtlintExtension> {
    version.set("0.41.0")
}

tasks.register("lint") {
    dependsOn("ktlintCheck")
}
