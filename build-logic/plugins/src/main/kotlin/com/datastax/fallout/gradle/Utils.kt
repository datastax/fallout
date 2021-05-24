@file:JvmName("Utils")

package com.datastax.fallout.gradle

fun camelCase(prefix: String, vararg components: String) =
    components.joinToString(prefix = prefix, separator = "") { it.capitalize() }
