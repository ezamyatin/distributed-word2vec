package ru.vkontakte.algorithm.word2vec.pair.generator;

import it.unimi.dsi.fastutil.longs.LongArrayList;
import ru.vkontakte.algorithm.word2vec.pair.LongPair;
import ru.vkontakte.algorithm.word2vec.pair.LongPairMulti;

import java.io.Serializable;

/**
 * @author ezamyatin
 **/
public class BatchedGenerator implements Serializable {
    private final int TOTAL_BATCH_SIZE = 10000000;

    private final PairGenerator pairGenerator;
    private final int batchSize;

    private final LongArrayList[] l, r;
    private int filled = -1;
    private int lastPtr = 0;

    public BatchedGenerator(PairGenerator pairGenerator) {
        this.pairGenerator = pairGenerator;

        l = new LongArrayList[pairGenerator.numPartitions()];
        r = new LongArrayList[pairGenerator.numPartitions()];

        this.batchSize = TOTAL_BATCH_SIZE / pairGenerator.numPartitions();
        for (int i = 0; i < pairGenerator.numPartitions(); ++i) {
            l[i] = new LongArrayList(batchSize);
            r[i] = new LongArrayList(batchSize);
        }
    }

    public void reset(long[] sent) {
        assert !pairGenerator.hasNext();
        pairGenerator.reset(sent);
    }

    public LongPairMulti next(boolean force) {
        assert force || lastPtr == 0;

        while (pairGenerator.hasNext() && filled == -1) {
            LongPair pair = pairGenerator.next();
            l[pair.part].add(pair.left);
            r[pair.part].add(pair.right);

            if (l[pair.part].size() >= batchSize) {
                filled = pair.part;
            }
        }

        if (filled == -1 && force) {
            while (lastPtr < l.length && l[lastPtr].isEmpty()) {
                lastPtr += 1;
            }

            if (lastPtr < l.length) {
                filled = lastPtr;
            }
        }

        if (filled != -1 && filled < l.length) {
            LongPairMulti result = new LongPairMulti(filled, l[filled].toLongArray(), r[filled].toLongArray());
            l[filled].clear();
            r[filled].clear();
            filled = -1;
            return result;
        }

        return null;
    }
}
