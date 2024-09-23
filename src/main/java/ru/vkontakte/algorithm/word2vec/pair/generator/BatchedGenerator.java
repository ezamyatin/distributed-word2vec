package ru.vkontakte.algorithm.word2vec.pair.generator;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import ru.vkontakte.algorithm.word2vec.SkipGramUtil;
import ru.vkontakte.algorithm.word2vec.pair.LongPair;
import ru.vkontakte.algorithm.word2vec.pair.LongPairMulti;
import ru.vkontakte.algorithm.word2vec.pair.SamplingMode;
import ru.vkontakte.algorithm.word2vec.pair.SkipGramPartitioner;

import java.io.Serializable;
import java.util.Iterator;

/**
 * @author ezamyatin
 **/
public class BatchedGenerator implements Serializable {
    private final int TOTAL_BATCH_SIZE = 10000000;

    private final PairGenerator pairGenerator;
    private final int batchSize;

    private final LongArrayList[] l, r;

    public BatchedGenerator(PairGenerator pairGenerator) {
        this.pairGenerator = pairGenerator;
        assert pairGenerator.partitioner1().getNumPartitions() == pairGenerator.partitioner2().getNumPartitions();

        l = new LongArrayList[pairGenerator.partitioner1().getNumPartitions()];
        r = new LongArrayList[pairGenerator.partitioner1().getNumPartitions()];

        this.batchSize = TOTAL_BATCH_SIZE / pairGenerator.partitioner1().getNumPartitions();
        for (int i = 0; i < pairGenerator.partitioner1().getNumPartitions(); ++i) {
            l[i] = new LongArrayList(batchSize);
            r[i] = new LongArrayList(batchSize);
        }
    }


    public Iterator<LongPairMulti> generate(long[] sent) {
        return new UntilNullIterator<LongPairMulti>() {

            private final Iterator<LongPair> it = pairGenerator.generate(sent);
            private int filled = -1;

            @Override
            public LongPairMulti generateOrNull() {
                while (it.hasNext() && filled == -1) {
                    LongPair pair = it.next();
                    int part = pair.part;
                    l[part].add(pair.left);
                    r[part].add(pair.right);

                    if (l[part].size() >= batchSize) {
                        filled = part;
                    }
                }

                if (filled != -1) {
                    LongPairMulti result = new LongPairMulti(filled, l[filled].toLongArray(), r[filled].toLongArray());
                    l[filled].clear();
                    r[filled].clear();
                    filled = -1;
                    return result;
                }

                return null;
            }
        };
    }

    public Iterator<LongPairMulti> flush() {
        return new UntilNullIterator<LongPairMulti>() {
            int ptr = 0;

            @Override
            public LongPairMulti generateOrNull() {
                while (ptr < l.length && l[ptr].isEmpty()) {
                    ptr += 1;
                }

                if (ptr < l.length) {
                    LongPairMulti result = new LongPairMulti(ptr, l[ptr].toLongArray(), r[ptr].toLongArray());
                    l[ptr].clear();
                    r[ptr].clear();
                    return result;
                }

                return null;
            }
        };
    }
}
