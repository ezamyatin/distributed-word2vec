package com.github.ezamyatin.logfac.pair.generator;

import com.github.ezamyatin.logfac.pair.LongPair;
import com.github.ezamyatin.logfac.pair.LongPairMulti;
import it.unimi.dsi.fastutil.floats.FloatArrayList;
import it.unimi.dsi.fastutil.longs.LongArrayList;

import java.io.Serializable;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * @author ezamyatin
 */
public class BatchedGenerator implements Iterator<LongPairMulti>, Serializable {
    private final int TOTAL_BATCH_SIZE = 10000000;

    private final Iterator<LongPair> pairGenerator;
    private final int batchSize;

    private final LongArrayList[] left, right;
    private final FloatArrayList[] label, weight;

    private int nonEmptyCounter = 0;
    private int ptr = 0;

    public BatchedGenerator(Iterator<LongPair> pairGenerator,
                            int numPartitions,
                            boolean withLabel,
                            boolean withWeight) {
        this.pairGenerator = pairGenerator;

        left = new LongArrayList[numPartitions];
        right = new LongArrayList[numPartitions];
        if (withLabel) {
            label = new FloatArrayList[numPartitions];
        } else {
            label = null;
        }

        if (withWeight) {
            weight = new FloatArrayList[numPartitions];
        } else {
            weight = null;
        }

        this.batchSize = TOTAL_BATCH_SIZE / numPartitions;
        for (int i = 0; i < numPartitions; ++i) {
            left[i] = new LongArrayList(batchSize);
            right[i] = new LongArrayList(batchSize);
            if (label != null) {
                label[i] = new FloatArrayList(batchSize);
            }
            if (weight != null) {
                weight[i] = new FloatArrayList(batchSize);
            }
        }
    }

    @Override
    public boolean hasNext() {
        return pairGenerator.hasNext() || nonEmptyCounter > 0;
    }

    @Override
    public LongPairMulti next() {
        while (pairGenerator.hasNext()) {
            LongPair pair = pairGenerator.next();
            int part = pair.part;

            if (left[part].isEmpty()) {
                nonEmptyCounter += 1;
            }

            left[part].add(pair.left);
            right[part].add(pair.right);

            if (label != null) {
                label[part].add(pair.label);
            }

            if (weight != null) {
                weight[part].add(pair.weight);
            }

            if (left[part].size() >= batchSize) {
                LongPairMulti result = new LongPairMulti(part, left[part].toLongArray(), right[part].toLongArray(),
                        label == null ? null : label[part].toFloatArray(),
                        weight == null ? null : weight[part].toFloatArray());
                left[part].clear();
                right[part].clear();
                if (label != null) {
                    label[part].clear();
                }
                if (weight != null) {
                    weight[part].clear();
                }
                nonEmptyCounter -= 1;
                return result;
            }
        }

        while (ptr < left.length && left[ptr].isEmpty()) {
            ptr += 1;
        }

        if (ptr < left.length) {
            LongPairMulti result = new LongPairMulti(ptr, left[ptr].toLongArray(), right[ptr].toLongArray(),
                    label == null ? null : label[ptr].toFloatArray(),
                    weight == null ? null : weight[ptr].toFloatArray());
            left[ptr].clear();
            right[ptr].clear();
            if (label != null) {
                label[ptr].clear();
            }
            if (weight != null) {
                weight[ptr].clear();
            }
            nonEmptyCounter -= 1;
            return result;
        }

        throw new NoSuchElementException("next on empty iterator");
    }
}
