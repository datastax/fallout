/** Common settings for projects containing kotlin code */

package com.datastax.fallout.conventions

plugins {
    id("org.jlleitschuh.gradle.ktlint")
    id("com.datastax.fallout.conventions.lint")
}

tasks.named("lint").configure {
    dependsOn("ktlintCheck")
}
