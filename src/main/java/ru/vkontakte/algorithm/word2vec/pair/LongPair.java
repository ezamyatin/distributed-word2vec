package ru.vkontakte.algorithm.word2vec.pair;

import java.io.Serializable;

/**
 * @author ezamyatin
 **/
public class LongPair implements Serializable {
    public final int part;
    public final long left;
    public final long right;

    public LongPair(int part, long left, long right) {
        this.part = part;
        this.left = left;
        this.right = right;
    }
}
