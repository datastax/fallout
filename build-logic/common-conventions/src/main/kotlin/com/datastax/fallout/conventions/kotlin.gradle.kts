/** Common settings for projects containing kotlin code */

package com.datastax.fallout.conventions

import com.datastax.fallout.gradle.common.maybeRegister
import org.gradle.kotlin.dsl.*

plugins {
    id("org.jlleitschuh.gradle.ktlint")
}

tasks.maybeRegister<DefaultTask>("lint") {
    dependsOn("ktlintCheck")
}
