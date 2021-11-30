buildscript {
    configurations.classpath {
        resolutionStrategy.activateDependencyLocking()
    }
}

plugins {
    `kotlin-dsl`

    id("org.jlleitschuh.gradle.ktlint") version "10.1.0"
}

group = "com.datastax.fallout"

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

dependencyLocking {
    lockAllConfigurations()
}

// Frustratingly, there doesn't appear to be an easy (i.e. non-baroque) way to DRY this
// up further: the tasks that this project defines in convention plugins for use in
// other projects can't be reused in this project, so we end up creating them again here:

// lint.gradle.kts

tasks {
    register("lint") {
        dependsOn(matching { it.name.startsWith("compile") })
    }
}

// kotlin.gradle.kts

configure<org.jlleitschuh.gradle.ktlint.KtlintExtension> {
    filter {
        exclude { element -> element.file.startsWith(project.buildDir) }
    }
}

tasks {
    named("lint") {
        dependsOn("ktlintCheck")
    }
}

// dependency-locking.gradle.kts

tasks {
    register("lockDependencies") {
        doLast {
            require(project.gradle.startParameter.isWriteDependencyLocks) {
                "$name must be called with the --write-locks option"
            }
            project.configurations.filter {
                it.isCanBeResolved
            }.forEach { it.resolve() }
        }
    }
}
