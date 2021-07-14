package com.datastax.fallout.gradle.fork

import org.gradle.api.internal.file.FileResolver
import org.gradle.api.tasks.AbstractExecTask
import org.gradle.api.tasks.Internal
import org.gradle.internal.concurrent.DefaultExecutorFactory
import org.gradle.internal.concurrent.ExecutorFactory
import org.gradle.internal.file.PathToFileResolver
import org.gradle.kotlin.dsl.getByType
import org.gradle.process.BaseExecSpec
import org.gradle.process.ExecSpec
import org.gradle.process.internal.DefaultExecHandleBuilder
import org.gradle.process.internal.ExecHandle
import org.gradle.process.internal.ExecHandleState
import java.util.concurrent.Executor

/** Similar to [org.gradle.process.internal.DefaultExecAction], but instead of
 *  [org.gradle.process.internal.DefaultExecAction.execute] we do [fork].  */
class ForkAction(pathToFileResolver: PathToFileResolver, executor: Executor) :
    DefaultExecHandleBuilder(pathToFileResolver, executor) {

    fun fork() = ForkHandle(build().start())
}

/** The single instance of this is created by [ForkPlugin] */
open class ForkActionFactory(private val fileResolver: FileResolver) {
    private val executorFactory: ExecutorFactory = DefaultExecutorFactory()

    fun create(): ForkAction {
        return ForkAction(fileResolver, executorFactory.create("gradle-processes"))
    }
}

/**
 * Wrapper class for converting internal ExecHandler reference to a ProcessHandle
 */
class ForkHandle(private val execHandle: ExecHandle) {
    val state: ExecHandleState
        get() = execHandle.state

    fun waitForFinish() =
        execHandle.waitForFinish()!!

    fun abort() {
        execHandle.abort()
    }
}

/**
 * Gradle task that creates a Forked process and provides an access point for capturing the process handle in
 * subsequent tasks
 */
open class Fork : AbstractExecTask<Fork>(Fork::class.java) {
    private val forkAction = project.extensions.getByType(ForkActionFactory::class).create()

    @get:Internal
    var processHandle: ForkHandle? = null
        private set

    /** Copied from [DefaultExecSpec.copyTo] */
    open fun copyTo(targetSpec: ExecSpec) {
        // Fork options
        super.copyTo(targetSpec)
        // BaseExecSpec
        copyBaseExecSpecTo(this, targetSpec)
        // ExecSpec
        targetSpec.args = args
        targetSpec.argumentProviders.addAll(argumentProviders)
    }

    /** Copied from [DefaultExecSpec.copyBaseExecSpecTo] */
    open fun copyBaseExecSpecTo(source: BaseExecSpec, target: BaseExecSpec) {
        target.isIgnoreExitValue = source.isIgnoreExitValue
        if (source.standardInput != null) {
            target.standardInput = source.standardInput
        }
        if (source.standardOutput != null) {
            target.standardOutput = source.standardOutput
        }
        if (source.errorOutput != null) {
            target.errorOutput = source.errorOutput
        }
    }

    override fun exec() {
        copyTo(forkAction)
        processHandle = forkAction.fork()
    }
}
