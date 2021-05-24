package com.datastax.fallout.gradle.symlinks.tasks

import com.datastax.fallout.gradle.symlinks.SymlinkCopyActionDecorator
import org.gradle.api.internal.file.copy.CopyAction
import org.gradle.api.tasks.Copy

/** Like [Copy], but doesn't dereference symlinks */
open class CopySymlinks : Copy() {
    override fun createCopyAction(): CopyAction {
        return SymlinkCopyActionDecorator(
            fileLookup.getFileResolver(destinationDir),
            super.createCopyAction()
        )
    }
}
