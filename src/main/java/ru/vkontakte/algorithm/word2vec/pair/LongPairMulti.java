package ru.vkontakte.algorithm.word2vec.pair;

import it.unimi.dsi.fastutil.longs.Long2IntOpenHashMap;

import java.io.Serializable;

/**
 * @author ezamyatin
 **/
public class LongPairMulti implements Serializable {

    public final int part;

    public final long[] l;

    public final long[] r;

    public LongPairMulti remap(Long2IntOpenHashMap vocabL, Long2IntOpenHashMap vocabR) {
        for (int i = 0; i < l.length; i++) {
            l[i] = vocabL.getOrDefault(l[i], -1);
            r[i] = vocabR.getOrDefault(r[i], -1);
        }
        return this;
    }

    public LongPairMulti(int part, long[] l, long[] r) {
        this.part = part;
        this.l = l;
        this.r = r;
    }
}
