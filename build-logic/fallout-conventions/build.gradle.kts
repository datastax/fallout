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

dependencies {
    implementation("com.datastax:build-logic-common-conventions")
}

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

// Resources for use by PluginInfo

val generatedMainResourcesOutputDir = file("$buildDir/src/main/resources")

sourceSets["main"].resources.srcDirs(generatedMainResourcesOutputDir)

val generatePluginInfo by tasks.registering {

    val content = projectDir.toString()
    val target = file("$generatedMainResourcesOutputDir/com/datastax/fallout/gradle/conventions/project-dir")

    inputs.property("content", content)
    outputs.file(target)

    doLast {
        target.parentFile.mkdirs()
        target.writeText(content)
    }
}

tasks["processResources"].dependsOn(generatePluginInfo)
