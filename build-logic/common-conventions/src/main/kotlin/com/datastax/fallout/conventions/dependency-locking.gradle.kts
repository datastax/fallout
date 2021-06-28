/** Common dependency locking tasks: this makes it possible to update all the .lockfiles with a single
 *  invocation of `./gradlew lockDependencies --write-locks` at the root level.  As of Gradle 7.1, without
 *  this we have to invoke `./gradlew dependencies --write-locks` on every project individually. */

package com.datastax.fallout.conventions

import com.datastax.fallout.gradle.common.LockDependencies
import com.datastax.fallout.gradle.common.cascadeTask

dependencyLocking {
    lockAllConfigurations()
}

val lockDependencies by tasks.registering(LockDependencies::class)

cascadeTask(project, lockDependencies)
