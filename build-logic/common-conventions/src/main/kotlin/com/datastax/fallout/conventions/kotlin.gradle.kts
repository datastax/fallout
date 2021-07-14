/** Common settings for projects containing kotlin code */

package com.datastax.fallout.conventions

plugins {
    id("org.jlleitschuh.gradle.ktlint")
    id("com.datastax.fallout.conventions.lint")
}

tasks.named("lint").configure {
    dependsOn("ktlintCheck")
}

configure<org.jlleitschuh.gradle.ktlint.KtlintExtension> {
    filter {
        // Exclude generated sources (the <convention>.gradle.kts files will have .kt
        // files generated for them); a plain glob of **/build/** fails because it's
        // applied to the source path _within the source dir_, which won't include /build/.
        exclude { element -> element.file.startsWith(project.buildDir) }
    }
}
