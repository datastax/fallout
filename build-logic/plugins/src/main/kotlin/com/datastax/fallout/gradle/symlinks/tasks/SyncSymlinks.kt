package com.datastax.fallout.gradle.symlinks.tasks

import com.datastax.fallout.gradle.symlinks.SymlinkCopyActionDecorator
import org.gradle.api.internal.file.copy.CopyAction
import org.gradle.api.tasks.Sync

/** Like [Sync], but doesn't dereference symlinks */
open class SyncSymlinks : Sync() {
    override fun createCopyAction(): CopyAction {
        return SymlinkCopyActionDecorator(
            fileLookup.getFileResolver(destinationDir),
            super.createCopyAction()
        )
    }
}
