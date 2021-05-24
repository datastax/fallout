dependencyResolutionManagement {
    repositoriesMode.set(RepositoriesMode.PREFER_SETTINGS)
    repositories {
        mavenCentral()

        // Needed for jepsen/clojure
        maven(uri("https://repo.clojars.org"))

        // Needed for com.github.nitsanw:HdrLogProcessing
        maven(uri("https://jitpack.io"))
    }
}

rootProject.name = "fallout"
includeBuild("build-logic/test-conventions")
includeBuild("build-logic/plugins")
includeBuild("build-logic/conventions")
include("cassandra-all-shaded")
include("jepsen")
include(":tools:artifact-checkers:no-op")
include(":tools:artifact-checkers:mttr")
