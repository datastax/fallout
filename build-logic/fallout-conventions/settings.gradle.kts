dependencyResolutionManagement {
    // This should be FAIL_ON_PROJECT_REPOS; we're using PREFER_SETTINGS
    // until https://github.com/gradle/gradle/issues/15732 is fixed.
    @Suppress("UnstableApiUsage")
    repositoriesMode.set(RepositoriesMode.PREFER_SETTINGS)

    @Suppress("UnstableApiUsage")
    repositories {
        mavenCentral()
        gradlePluginPortal()
    }
}

enableFeaturePreview("ONE_LOCKFILE_PER_PROJECT")

rootProject.name = "build-logic-fallout-conventions"

includeBuild("../common-conventions")
