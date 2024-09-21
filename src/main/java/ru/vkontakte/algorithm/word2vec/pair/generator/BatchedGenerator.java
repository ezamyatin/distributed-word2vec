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
    private final SkipGramPartitioner partitioner1;
    private final SkipGramPartitioner partitioner2;

    public BatchedGenerator(PairGenerator pairGenerator,
                            SkipGramPartitioner partitioner1,
                            SkipGramPartitioner partitioner2) {
        this.partitioner1 = partitioner1;
        this.partitioner2 = partitioner2;
        this.pairGenerator = new PairGenerator() {
            private IntArrayList p1 = new IntArrayList(1000);
            private IntArrayList p2 = new IntArrayList(1000);

            @Override
            public Iterator<LongPair> generate(long[] sent) {
                p1.clear();
                p2.clear();
                for (long s : sent) {
                    p1.add(partitioner1.getPartition(s));
                    p2.add(partitioner2.getPartition(s));
                }
                return pairGenerator.generate(sent);
            }

            @Override
            public boolean skipPair(long[] sent, int i, int j, SamplingMode samplingMode) {
                return p1.getInt(i) != p2.getInt(j) || PairGenerator.super.skipPair(sent, i, j, samplingMode);
            }
        };

        assert partitioner1.getNumPartitions() == partitioner2.getNumPartitions();

        l = new LongArrayList[partitioner1.getNumPartitions()];
        r = new LongArrayList[partitioner1.getNumPartitions()];

        this.batchSize = TOTAL_BATCH_SIZE / partitioner1.getNumPartitions();
        for (int i = 0; i < partitioner1.getNumPartitions(); ++i) {
            l[i] = new LongArrayList(batchSize);
            r[i] = new LongArrayList(batchSize);
        }
    }


    public Iterator<LongPairMulti> generate(long[] sent) {
        return SkipGramUtil.untilNull(new Iterator<LongPairMulti>() {
            Iterator<LongPair> it = pairGenerator.generate(sent);
            private int filled = -1;

            @Override
            public boolean hasNext() {
                return true;
            }

            @Override
            public LongPairMulti next() {
                while (it.hasNext() && filled == -1) {
                    LongPair pair = it.next();
                    int part = partitioner1.getPartition(pair.left);
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
        });
    }

    public Iterator<LongPairMulti> flush() {
        return SkipGramUtil.untilNull(new Iterator<LongPairMulti>() {
            int ptr = 0;

            @Override
            public boolean hasNext() {
                return true;
            }

            @Override
            public LongPairMulti next() {
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
        });
    }
}
