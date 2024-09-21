package ru.vkontakte.algorithm.word2vec.pair;

import java.io.Serializable;

/**
 * @author ezamyatin
 **/
public class LongPairMulti implements Serializable {

    public final int part;

    public final long[] l;

    public final long[] r;

    public LongPairMulti(int part, long[] l, long[] r) {
        this.part = part;
        this.l = l;
        this.r = r;
    }
}
