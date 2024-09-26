package ru.vkontakte.algorithm.word2vec.pair;

import java.io.Serializable;

/**
 * @author ezamyatin
 **/
public class LongPair implements Serializable {
    public final static float EMPTY_RATING = Float.NaN;

    public final int part;
    public final long left;
    public final long right;
    public final float rating;

    public LongPair(int part, long left, long right, float rating) {
        this.part = part;
        this.left = left;
        this.right = right;
        this.rating = rating;
    }

    public LongPair(int part, long left, long right) {
        this.part = part;
        this.left = left;
        this.right = right;
        this.rating = EMPTY_RATING;
    }
}
