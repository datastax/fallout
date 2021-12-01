buildscript {
    configurations.classpath {
        resolutionStrategy.activateDependencyLocking()
    }
}

plugins {
    `kotlin-dsl`

    id("com.datastax.fallout.conventions.test")
    id("com.datastax.fallout.conventions.kotlin")
    id("com.datastax.fallout.conventions.dependency-locking")
}

group = "com.datastax.fallout"

dependencies {
    implementation("de.undercouch:gradle-download-task:4+")
    implementation("com.google.gradle:osdetector-gradle-plugin:1+")
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

        create("fork") {
            id = "com.datastax.fallout.fork"
            implementationClass = "com.datastax.fallout.gradle.fork.ForkPlugin"
        }

        create("git") {
            id = "com.datastax.fallout.git"
            implementationClass = "com.datastax.fallout.gradle.git.GitPlugin"
        }

        create("docker") {
            id = "com.datastax.fallout.docker"
            implementationClass = "com.datastax.fallout.gradle.docker.DockerPlugin"
        }
    }
}
