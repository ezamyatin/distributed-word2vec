package ru.vkontakte.mf.pair;

import it.unimi.dsi.fastutil.longs.Long2IntOpenHashMap;

import javax.annotation.Nullable;
import java.io.Serializable;

/**
 * @author ezamyatin
 **/
public class LongPairMulti implements Serializable {

    public final int part;

    public final long[] left;

    public final long[] right;

    public final float[] label;

    public final float[] weight;

    public LongPairMulti remap(Long2IntOpenHashMap vocabL, Long2IntOpenHashMap vocabR) {
        for (int i = 0; i < left.length; i++) {
            left[i] = vocabL.getOrDefault(left[i], -1);
            right[i] = vocabR.getOrDefault(right[i], -1);
        }
        return this;
    }

    public LongPairMulti(int part, long[] left, long[] right, @Nullable float[] label, @Nullable float[] weight) {
        this.part = part;
        this.left = left;
        this.right = right;
        this.label = label;
        this.weight = weight;
    }
}
