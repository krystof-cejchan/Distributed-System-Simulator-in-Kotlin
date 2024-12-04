package cz.krystofcejchan.utils

/**
 * Set má k získání elementu funkci elementAt...to je takové nepřirozené, takže si tady definuji get funkci
 * @see Set#elementAt
 */
operator fun <E> Set<E>.get(nextIndex: Int): E {
    return this.elementAt(nextIndex)
}
