@file:Suppress("UnstableApiUsage")

package com.datastax.fallout.gradle.externaltools

import com.datastax.fallout.gradle.camelCase
import com.datastax.fallout.gradle.symlinks.tasks.Symlink
import com.google.gradle.osdetector.OsDetector
import groovy.lang.Closure
import org.gradle.api.Action
import org.gradle.api.DefaultTask
import org.gradle.api.Named
import org.gradle.api.Project
import org.gradle.api.Task
import org.gradle.api.file.Directory
import org.gradle.api.file.DirectoryProperty
import org.gradle.api.provider.Provider
import org.gradle.api.tasks.Copy
import org.gradle.api.tasks.TaskProvider
import org.gradle.kotlin.dsl.*
import java.util.function.BiFunction
import kotlin.reflect.KClass

private val platformsMap = mapOf(
    "darwin" to "osx"
)

private fun normalizePlatform(platform: String) =
    platformsMap.getOrDefault(platform, platform)

private const val SHARED_PLATFORM_NAME = "shared"

/** Replacement for [ConfigureUtil.configureUsing], since the latter has been deprecated in Gradle 7; the
 * official advice is to ensure new objects are created using [org.gradle.api.model.ObjectFactory.newInstance],
 * which auto-creates groovy overloads for methods with [Action] parameters (see
 * https://github.com/gradle/gradle/blob/a67d0a570f8f3cfbf1e514479fed337dcfb256c7/subprojects/model-core/src/main/java/org/gradle/util/ConfigureUtil.java#L64).
 *  However, this doesn't work for inner classes, and inner classes are very convenient for this implementation, so we stick
 * with using our own overloads. */
private fun <T> configureUsing(project: Project, configure: Closure<in Any>): Action<T> =
    object : Action<T> {
        override fun execute(t: T) {
            project.configure(t as Any, configure)
        }
    }

/** Provides a declarative DSL for defining external tools; groovy example:
 *
 * ```
 * externalTools {
 *
 *     // Declare a configuration; these are semantically equivalent to standard gradle configurations in that they
 *     // gather a group of related components for use in a particular environment e.g. "main" for production, "test"
 *     // for test.  When a configuration name is used as part of a task name, if it's "main" then it will be omitted.
 *     //
 *     // For each unique platform (see below) declared in a tool, the following tasks will be created:
 *     //   - installExternalTools<Configuration><Platform>
 *     //   - installExternalToolsAll<Platform>
 *     //   - installExternalToolsAllNative
 *     configuration("main") {
 *
 *         // Declare a binary tool i.e. one that can be downloaded, marked executable, and run.
 *         binary("kubectl") {
 *
 *             // Tools are either specific to a platform ("linux", "darwin") or "shared".  Each time you say
 *             // platform or platforms, it allows you to set the properties for that platform for the current
 *             // tool.  Some platform names, when used in a task name, will be mapped internally to a common
 *             // name that matches the output of the osdetector plugin: for example, "darwin" is mapped to "osx".
 *             //
 *             // Each unique platform will create the following tasks
 *             //
 *             // - download<Tool><Platform>
 *             // - install<Tool><Platform> - this will do whatever is needed to make the tool executable
 *             platforms(["linux", "darwin"]) {
 *
 *                 // Within a platforms block, "platform" is a variable that expands to the
 *                 // current platform, so in this case we can use it to avoid typing the
 *                 // source of the kubectl download twice, since it only varies by platform.
 *                 source.set("https://dl.k8s.io/release/v1.16.0/bin/${platform}/amd64/kubectl")
 *             }
 *
 *             // Setting checksums increases security and also enables better build avoidance
 *             platform("linux") {
 *                 checksum.set("4fc8a7024ef17b907820890f11ba7e59a6a578fa91ea593ce8e58b3260f7fb88")
 *             }
 *
 *             platform("darwin") {
 *                 checksum.set("a81b23abe67e70f8395ff7a3659bea6610fba98cda1126ef19e0a995f0075d54")
 *             }
 *         }
 *     }
 * }
 * ```
 */
open class ExternalToolsExtension(private val project: Project) {
    private val currentOs = project.extensions.getByType<OsDetector>().os

    private inline fun <reified T : Task> registerTask(name: String, configure: Action<T>) =
        project.tasks.register<T>(name) {
            group = "External tools"
            configure.execute(this)
        }

    private val installAllTasks = HashMap<String, TaskProvider<DefaultTask>>()

    private fun installAllTask(platformName: String) = installAllTasks.computeIfAbsent(platformName) { _ ->
        registerTask(camelCase("installExternalToolsAll", platformName)) {
            description = "Install all tools for all configurations for $platformName"
        }
    }

    val installAllNativeTask = installAllTask(currentOs).let { installAllTaskCurrentOs ->
        registerTask<DefaultTask>("installExternalToolsAllNative") {
            description = "Install all tools for all configurations for $currentOs"
            dependsOn(installAllTaskCurrentOs)
        }
    }

    fun externalToolsDir(buildDirectory: DirectoryProperty) =
        buildDirectory.dir("externalTools")

    private fun configurationDir(
        configurationName: String,
        buildDirectory: DirectoryProperty = project.layout.buildDirectory
    ) = externalToolsDir(buildDirectory).map { it.dir(configurationName) }

    private fun platformDir(
        configurationName: String,
        platformName: String,
        buildDirectory: DirectoryProperty = project.layout.buildDirectory
    ) = configurationDir(configurationName, buildDirectory)
        .map { it.dir(normalizePlatform(platformName)) }

    fun platformInstallDir(
        configurationName: String,
        platformName: String,
        buildDirectory: DirectoryProperty = project.layout.buildDirectory
    ) = platformDir(configurationName, platformName, buildDirectory)
        .map { it.dir("install") }

    fun platformBinDir(
        configurationName: String,
        platformName: String,
        buildDirectory: DirectoryProperty = project.layout.buildDirectory
    ) = platformInstallDir(configurationName, platformName, buildDirectory)
        .map { it.dir("bin") }

    private fun platformDownloadDir(
        configurationName: String,
        platformName: String,
        buildDirectory: DirectoryProperty = project.layout.buildDirectory
    ) = platformDir(configurationName, platformName, buildDirectory)
        .map { it.dir("download") }

    /** This is semantically equivalent to gradle configurations, and allows us to collect tools for different
     *  purposes together e.g. test tools and main (i.e. production) tools. */
    open inner class Configuration(val name: String) {
        val configurationDir = configurationDir(name)

        private val configurationTaskNamePart = if (name == "main") {
            ""
        } else {
            name
        }

        private val platforms = project.objects.domainObjectContainer(Platform::class, ::Platform)

        private val sharedPlatform = platforms.create(SHARED_PLATFORM_NAME)

        fun platform(name: String) = platforms.get(name)

        /** String for inserting in a PATH variable that includes platform-specific binDir
         *  and the shared platform binDir */
        val nativePath = sharedPlatform.binDir.flatMap { sharedBinDir ->
            val p: Platform? = platforms.findByName(currentOs)

            p?.binDir?.map { currentOsBinDir ->
                "$sharedBinDir:$currentOsBinDir"
            } ?: project.provider { sharedBinDir.toString() }
        }

        open inner class Platform(val name: String) {
            val downloadDir = platformDownloadDir(this@Configuration.name, name)
            val installDir = platformInstallDir(this@Configuration.name, name)
            val libDir = installDir.map { it.dir("lib/tools/external") }
            val binDir = platformBinDir(this@Configuration.name, name)

            fun taskName(prefix: String) = camelCase(prefix, configurationTaskNamePart, name)

            val installTask = registerTask<DefaultTask>(taskName("installExternalTools")) {
                description = "Install all ${this@Configuration.name} tools for ${this@Platform.name}"

                outputs.dir(installDir)
            }

            private val isNotSharedPlatform = name != SHARED_PLATFORM_NAME

            // Make platform-specific install tasks depend on the shared platform install task
            init {
                if (isNotSharedPlatform) {
                    installTask.configure {
                        dependsOn(sharedPlatform.installTask)
                    }
                }
                installAllTask(name).configure {
                    dependsOn(installTask)
                }
            }
        }

        /** Defines the [downloadTask] for the `toolName` on a given OS platform, `name`;
         *  derived classes define extra tasks specific to a particular [Tool] */
        open inner class ToolPlatform(val name: String, val toolName: String) {

            /** The URL identifying the source of the tool */
            val source = project.objects.property<String>()

            /** A digest algorithm to be used for the checksum, for validation of the
             *  download and avoiding re-downloads.  Defaults to `sha-256` */
            val algorithm = DownloadTask.algorithmProperty(project.objects)

            /** The checksum to be used for validation/download avoidance.  If unset, then no validation happens and
             *  download avoidance is disabled */
            val checksum = project.objects.property<String>()

            /** Useful in the closure evaluated by [Tool.platforms]*/
            val platform = name

            val platformType = platforms.maybeCreate(normalizePlatform(name))

            protected fun relativeDir(dir: Provider<Directory>) = dir.get().asFile.relativeTo(project.rootDir)

            fun taskName(prefix: String) = platformType.taskName(camelCase(prefix, toolName))

            val downloadTask = registerTask<DownloadTask>(
                taskName("download")
            ) {
                description = "Download $toolName to ${relativeDir(platformType.downloadDir)}"

                source.set(this@ToolPlatform.source)
                val sourceFileName = this@ToolPlatform.source.map { it.split('/').last() }
                val destFilePath = platformType.downloadDir.flatMap { it.file(sourceFileName) }
                dest.set(destFilePath)
                algorithm.set(this@ToolPlatform.algorithm)
                checksum.set(this@ToolPlatform.checksum)
            }
        }

        /** DSL base class for the objects created by [Configuration.binary], [Configuration.tarball]
         *  etc.  Contains [ToolPlatform] instances, which are created by the [platform] DSL method */
        abstract inner class Tool<P : ToolPlatform>(
            private val name: String,
            platformClass: KClass<P>,
            platformFactory: BiFunction<String, String, P>
        ) : Named {

            override fun getName(): String = name

            private val platforms = project.objects.domainObjectContainer(platformClass) { platformName ->
                platformFactory.apply(platformName, name)
            }

            // DSL methods

            /** Add a platform or configure an existing platform for the current [Tool] */
            fun platform(name: String, configure: Action<P>) =
                platforms.maybeCreate(name).apply {
                    configure.execute(this)
                }

            /** Overload for groovy callers */
            fun platform(name: String, configure: Closure<in Any>) =
                platform(name, configureUsing(project, configure))

            /** Shortcut syntax to add/configure multiple platforms */
            fun platforms(names: List<String>, configure: Action<P>) {
                names.forEach { platform(it, configure) }
            }

            /** Overload for groovy callers */
            fun platforms(names: List<String>, configure: Closure<in Any>) {
                names.forEach { platform(it, configure) }
            }
        }

        /** Defines a binary tool that just needs to be copied into place */
        open inner class BinaryToolPlatform(name: String, toolName: String) : ToolPlatform(name, toolName) {
            init {
                val installTask = registerTask<Copy>(taskName("install")) {
                    description = "Copy downloaded $toolName to ${relativeDir(platformType.binDir)}"

                    dependsOn(downloadTask)

                    from(downloadTask.map { it.outputs.files.singleFile })
                    into(platformType.binDir)
                    fileMode = Integer.valueOf("755", 8)

                    // Using rename makes this task uncacheable; there's no other way to rename the file with
                    // a Copy task unfortunately.
                    rename { toolName }
                }

                platformType.installTask.configure {
                    dependsOn(installTask)
                }
            }
        }

        open inner class BinaryTool(name: String) :
            Tool<BinaryToolPlatform>(name, BinaryToolPlatform::class, ::BinaryToolPlatform)

        /** Defines a tool that is packaged in a tarball, and must be unpacked and then symlinked
         *  into place */
        open inner class TarballToolPlatform(name: String, toolName: String) : ToolPlatform(name, toolName) {
            /** Set the path within the tarball that contains the tool; leave unset if the tool is
             *  in the top-level of the tarball */
            val unpackedBinDir = project.objects.property<String>()

            /** Set the name that will be used for the symlink; defaults to the tool name */
            val symlinkName = project.objects.property<String>().apply {
                convention(toolName)
            }

            init {
                val toolInstallDir = platformType.libDir.map { it.dir(toolName) }
                val unpackedToolFile = toolInstallDir.flatMap { toolInstallDir_ ->
                    unpackedBinDir
                        .map {
                            toolInstallDir_.dir(it)
                        }
                        .orElse(toolInstallDir_)
                        .map {
                            it.file(toolName)
                        }
                }

                val unpackTask = registerTask<Copy>(taskName("unpack")) {
                    description = "Unpack downloaded $toolName to ${relativeDir(toolInstallDir)}"

                    dependsOn(downloadTask)

                    from(downloadTask.map {
                        project.tarTree(project.resources.gzip(it.dest))
                    })

                    into(toolInstallDir)

                    // The Copy task cannot infer this from the output of tartree
                    inputs.file(downloadTask.map { it.dest })
                }

                val installTask = registerTask<Symlink>(taskName("install")) {
                    description = "Create a symlink for $toolName in ${relativeDir(platformType.binDir)}"

                    dependsOn(unpackTask)

                    val destFile = platformType.binDir.flatMap { binDir ->
                        symlinkName.map { symlinkName ->
                            binDir.file(symlinkName)
                        }
                    }

                    dest.set(destFile)

                    source.set(
                        destFile.flatMap { destFile_ ->
                            unpackedToolFile.map { toolFile ->
                                toolFile.asFile.toRelativeString(destFile_.asFile.parentFile)
                            }
                        }
                    )
                }

                platformType.installTask.configure {
                    dependsOn(installTask)
                }
            }
        }

        open inner class TarballTool(name: String) :
            Tool<TarballToolPlatform>(name, TarballToolPlatform::class, ::TarballToolPlatform)

        private val tools = project.objects.polymorphicDomainObjectContainer(Tool::class)

        init {
            tools.registerFactory(BinaryTool::class.java) {
                BinaryTool(it)
            }
            tools.registerFactory(TarballTool::class.java) {
                TarballTool(it)
            }
        }

        // DSL methods

        /** Add a [BinaryTool] */
        fun binary(name: String, configure: Action<BinaryTool>) =
            tools.create(name, BinaryTool::class.java, configure)

        /** Overload for groovy callers */
        fun binary(name: String, configure: Closure<in Any>) =
            binary(name, configureUsing(project, configure))

        /** Add a [TarballTool] */
        fun tarball(name: String, configure: Action<TarballTool>) =
            tools.create(name, TarballTool::class.java, configure)

        /** Overload for groovy callers */
        fun tarball(name: String, configure: Closure<in Any>) =
            tarball(name, configureUsing(project, configure))
    }

    private val configurations = project.objects.domainObjectContainer(Configuration::class) { configurationName ->
        Configuration(configurationName)
    }

    /** Return a path that contains all the tools for [currentOs]; needed for testing */
    fun getNativePath() = configurations.map { it.nativePath.get() }.joinToString(separator = ":")

    /** Used to construct a native path for composed builds */
    fun getNativePath(
        configurations: List<String>,
        buildDirectory: DirectoryProperty = project.layout.buildDirectory
    ) =
        configurations
            .flatMap {
                listOf(
                    platformBinDir(it, currentOs, buildDirectory),
                    platformBinDir(it, SHARED_PLATFORM_NAME, buildDirectory)
                )
            }.joinToString(":") { it.get().toString() }

    // DSL methods

    /** Create a [Configuration] */
    fun configuration(name: String, configure: Action<Configuration>) {
        configurations.create(name, configure)
    }

    fun configuration(name: String) = configurations.get(name)
}
