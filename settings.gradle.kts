dependencyResolutionManagement {
    // This should be FAIL_ON_PROJECT_REPOS; we're using PREFER_SETTINGS
    // until https://github.com/gradle/gradle/issues/15732 is fixed.
    @Suppress("UnstableApiUsage")
    repositoriesMode.set(RepositoriesMode.PREFER_SETTINGS)

    @Suppress("UnstableApiUsage")
    repositories {
        mavenCentral()

        // Needed for jepsen/clojure
        maven(uri("https://repo.clojars.org"))

        // Needed for com.github.nitsanw:HdrLogProcessing
        maven(uri("https://jitpack.io"))
    }
}

enableFeaturePreview("ONE_LOCKFILE_PER_PROJECT")

rootProject.name = "fallout"
includeBuild("build-logic/common-conventions")
includeBuild("build-logic/plugins")
includeBuild("build-logic/fallout-conventions")
include("cassandra-all-shaded")
include("jepsen")
include(":tools:artifact-checkers:no-op")
include(":tools:artifact-checkers:mttr")
