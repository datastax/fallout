package com.datastax.fallout.gradle.conventions

object PluginInfo {
    fun getProjectDir() = PluginInfo::class.java.getResource("project-dir")!!.readText()
}
