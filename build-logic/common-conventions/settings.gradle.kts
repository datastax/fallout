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

rootProject.name = "build-logic-common-conventions"
