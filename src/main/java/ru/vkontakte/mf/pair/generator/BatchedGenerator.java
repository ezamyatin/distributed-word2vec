package ru.vkontakte.mf.pair.generator;

import it.unimi.dsi.fastutil.floats.FloatArrayList;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import ru.vkontakte.mf.pair.LongPairMulti;
import ru.vkontakte.mf.pair.LongPair;

import java.io.Serializable;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * @author ezamyatin
 **/
public class BatchedGenerator implements Iterator<LongPairMulti>, Serializable {
    private final int TOTAL_BATCH_SIZE = 10000000;

    private final Iterator<LongPair> pairGenerator;
    private final int batchSize;

    private final LongArrayList[] l, r;
    private final FloatArrayList[] w;

    private int nonEmptyCounter = 0;
    private int ptr = 0;

    public BatchedGenerator(Iterator<LongPair> pairGenerator,
                            int numPartitions,
                            boolean withRating) {
        this.pairGenerator = pairGenerator;

        l = new LongArrayList[numPartitions];
        r = new LongArrayList[numPartitions];
        if (withRating) {
            w = new FloatArrayList[numPartitions];
        } else {
            w = null;
        }

        this.batchSize = TOTAL_BATCH_SIZE / numPartitions;
        for (int i = 0; i < numPartitions; ++i) {
            l[i] = new LongArrayList(batchSize);
            r[i] = new LongArrayList(batchSize);
            if (w != null) {
                w[i] = new FloatArrayList(batchSize);
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

            if (l[part].isEmpty()) {
                nonEmptyCounter += 1;
            }

            l[part].add(pair.left);
            r[part].add(pair.right);
            if (w != null) {
                w[part].add(pair.rating);
            }

            if (l[part].size() >= batchSize) {
                LongPairMulti result = new LongPairMulti(part, l[part].toLongArray(), r[part].toLongArray(),
                        w == null ? null : w[part].toFloatArray());
                l[part].clear();
                r[part].clear();
                if (w != null) {
                    w[part].clear();
                }
                nonEmptyCounter -= 1;
                return result;
            }
        }

        while (ptr < l.length && l[ptr].isEmpty()) {
            ptr += 1;
        }

        if (ptr < l.length) {
            LongPairMulti result = new LongPairMulti(ptr, l[ptr].toLongArray(), r[ptr].toLongArray(),
                    w == null ? null : w[ptr].toFloatArray());
            l[ptr].clear();
            r[ptr].clear();
            if (w != null) {
                w[ptr].clear();
            }
            nonEmptyCounter -= 1;
            return result;
        }

        throw new NoSuchElementException("next on empty iterator");
    }
}
