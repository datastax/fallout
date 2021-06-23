dependencyResolutionManagement {
    @Suppress("UnstableApiUsage")
    repositories {
        mavenCentral()
        gradlePluginPortal()
    }
}

enableFeaturePreview("ONE_LOCKFILE_PER_PROJECT")

rootProject.name = "build-logic-fallout-conventions"

includeBuild("../common-conventions")
