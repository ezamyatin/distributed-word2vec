package ru.vkontakte.algorithm.word2vec.pair;

import java.io.Serializable;

/**
 * @author ezamyatin
 **/
public class LongPair implements Serializable {
    public final long left;
    public final long right;

    public LongPair(long left, long right) {
        this.left = left;
        this.right = right;
    }
}
