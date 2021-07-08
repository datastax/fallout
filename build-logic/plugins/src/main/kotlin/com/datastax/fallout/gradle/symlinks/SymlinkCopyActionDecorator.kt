package com.datastax.fallout.gradle.symlinks

import org.gradle.api.internal.file.FileResolver
import org.gradle.api.internal.file.copy.CopyAction
import org.gradle.api.internal.file.copy.CopyActionProcessingStream
import org.gradle.api.tasks.WorkResult
import java.nio.file.Files

/** Copies symlinked files, delegating other copy actions to the delegate
 *
 *  This is kept as simple as possible: we _could_ make
 *  [CopySymlinks] and [SyncSymlinks] use different
 *  decorators that avoid always calling the delegated
 *  [org.gradle.api.internal.file.CopyActionProcessingStreamAction.processFile],
 *  but this would increase this code's reliance on
 *  internal gradle classes, which is a bad idea.
 */
class SymlinkCopyActionDecorator(
    private val fileResolver: FileResolver,
    private val delegate: CopyAction
) : CopyAction {

    override fun execute(stream: CopyActionProcessingStream): WorkResult {
        return delegate.execute { action ->
            stream.process { details ->

                // Delete the destination if it's a symlink: the default details.copyTo
                // tries to open a FileOutputStream on the target if it exists, and if
                // the target is a dangling symlink we get a FileNotFoundException.
                val destFile = fileResolver.resolve(details.relativePath).toPath()

                if (Files.isSymbolicLink(destFile)) {
                    Files.delete(destFile)
                }

                // we _always_ call the delegate: if we're wrapping a
                // SyncCopyActionDecorator, then it tracks all the
                // files visited and deletes the ones not seen, which
                // would delete all the symlinks created if we called it
                // conditionally
                action.processFile(details)

                // NormalizingCopyActionDecorator can stub out the details
                // parameter with its own internal type that throws
                // UnsupportedOperationException on getFile: see
                // https://github.com/gradle/gradle/blob/663ba1d8b0165521af9010bea62a7e48e637e10f/subprojects/core/src/main/java/org/gradle/api/internal/file/copy/NormalizingCopyActionDecorator.java#L125-L127
                // It doesn't affect the operation of this decorator, we just
                // need to stop it blowing up.
                //
                // This also means that copyspecs with content filtering
                // applied won't be applied, due to
                // https://github.com/gradle/gradle/blob/c7d513f6f652db01dba3f1d594feaa54a645b5ef/subprojects/core/src/main/java/org/gradle/api/internal/file/copy/DefaultFileCopyDetails.java#L72-L79
                val maybeSourceFile = try {
                    details.file.toPath()
                } catch (e: UnsupportedOperationException) {
                    null
                }

                maybeSourceFile?.let { sourceFile ->
                    if (Files.isSymbolicLink(sourceFile)) {
                        Files.delete(destFile)
                        Files.createSymbolicLink(destFile, Files.readSymbolicLink(sourceFile))
                    }
                }
            }
        }
    }
}
