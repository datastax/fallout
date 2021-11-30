buildscript {
    configurations.classpath {
        resolutionStrategy.activateDependencyLocking()
    }
}

plugins {
    `kotlin-dsl`

    id("com.datastax.fallout.conventions.kotlin")
    id("com.datastax.fallout.conventions.dependency-locking")
}

group = "com.datastax.fallout"

dependencies {
    implementation("com.datastax.fallout:build-logic-common-conventions")
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
