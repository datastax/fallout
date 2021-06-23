package com.datastax.fallout.conventions

import com.datastax.fallout.gradle.common.cascadeTask

project.afterEvaluate {

    cascadeTask(project, tasks.named("clean"))

    // Cascade dry-run settings; see https://github.com/gradle/gradle/issues/2517

    if (gradle.parent?.startParameter?.isDryRun == true) {
        gradle.startParameter.isDryRun = true
    }
}
