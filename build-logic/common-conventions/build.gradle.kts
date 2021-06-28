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

// Frustratingly, there doesn't appear to be an easy (i.e. non-baroque) way to DRY this
// up further: the tasks that this project defines in convention plugins for use in
// other projects can't be reused in this project, so we end up creating them again here:

tasks {
    register("lint") {
        dependsOn(matching { it.name.startsWith("compile") })
        dependsOn("ktlintCheck")
    }

    register("lockDependencies") {
        doLast {
            require(project.gradle.startParameter.isWriteDependencyLocks) {
                "$name must be called with the --write-locks option"
            }
            project.configurations.filter {
                // Add any custom filtering on the configurations to be resolved
                it.isCanBeResolved
            }.forEach { it.resolve() }
        }
    }
}
