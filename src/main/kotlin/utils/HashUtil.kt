package cz.krystofcejchan.utils

import com.google.common.hash.Hashing
import java.nio.charset.Charset

fun String.mmh3() = Hashing.murmur3_32_fixed(0).hashString(this, Charset.defaultCharset()).asInt()