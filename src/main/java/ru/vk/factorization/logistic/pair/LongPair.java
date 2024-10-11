package ru.vk.factorization.logistic.pair;

import java.io.Serializable;

/**
 * @author ezamyatin
 **/
public class LongPair implements Serializable {
    public final static float EMPTY = Float.NaN;

    public final int part;
    public final long left;
    public final long right;
    public final float label;
    public final float weight;

    public LongPair(int part, long left, long right, float label, float weight) {
        this.part = part;
        this.left = left;
        this.right = right;
        this.label = label;
        this.weight = weight;
    }

    public LongPair(int part, long left, long right) {
        this.part = part;
        this.left = left;
        this.right = right;
        this.label = EMPTY;
        this.weight = EMPTY;
    }
}
