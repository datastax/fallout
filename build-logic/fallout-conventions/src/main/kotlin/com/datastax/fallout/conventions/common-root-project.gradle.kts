package com.datastax.fallout.conventions

import com.datastax.fallout.gradle.common.maybeRegister

project.afterEvaluate {

    // Cascade the clean task to subprojects: normally, we wouldn't need this since `./gradlew clean` runs
    // `clean` in all subprojects, but because this build is sometimes included in others, and includedBuilds
    // don't support non-explicit task names, we special case clean since it means `./gradlew clean` is a
    // one-stop-shop for cleaning the project.

    val rootCleanTask = tasks.maybeRegister<DefaultTask>("clean")

    subprojects.forEach {
        it.afterEvaluate {
            try {
                tasks.named("clean")
            } catch (ex: UnknownTaskException) {
                null
            }?.let {
                rootCleanTask {
                    dependsOn(it)
                }
            }
        }
    }

    rootCleanTask {
        gradle.includedBuilds.forEach { includedBuild ->
            dependsOn(includedBuild.task(":clean"))
        }
    }

    // Cascade dry-run settings; see https://github.com/gradle/gradle/issues/2517

    if (gradle.parent?.startParameter?.isDryRun == true) {
        gradle.startParameter.isDryRun = true
    }
}
