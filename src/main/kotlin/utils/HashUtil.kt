package cz.krystofcejchan.utils

import com.google.common.hash.Hashing
import kotlin.math.absoluteValue

fun String.mmh3(): Int = Hashing.murmur3_32_fixed(0).hashBytes(this.toByteArray()).asInt().absoluteValue
