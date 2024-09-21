package ru.vkontakte.algorithm.word2vec.pair.generator;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import ru.vkontakte.algorithm.word2vec.pair.LongPair;
import ru.vkontakte.algorithm.word2vec.pair.SamplingMode;
import ru.vkontakte.algorithm.word2vec.pair.SkipGramPartitioner;

import java.util.Random;

/**
 * @author ezamyatin
 **/
public class Pos2NegPairGenerator implements PairGenerator {
    private final int window;
    private final SkipGramPartitioner partitioner1;
    private final SkipGramPartitioner partitioner2;
    private final SamplingMode samplingMode;
    private final Random random;

    private LongPair next = null;
    private int i = 0;
    private int j = 0;
    private int a = -1;

    private final IntArrayList partitionb;
    private final LongArrayList sentL, sentR;

    public Pos2NegPairGenerator(int window,
                                SkipGramPartitioner partitioner1,
                                SkipGramPartitioner partitioner2,
                                SamplingMode samplingMode,
                                int seed) {
        this.window = window;
        this.partitioner1 = partitioner1;
        this.partitioner2 = partitioner2;
        this.samplingMode = samplingMode;
        this.random = new Random(seed);
        this.partitionb = new IntArrayList(1000);
        this.sentL = new LongArrayList(1000);
        this.sentR = new LongArrayList(1000);
    }

    public void reset(long[] sent) {
        assert next == null;
        next = null;
        i = 0;
        j = 0;
        a = -1;
        partitionb.clear();
        sentL.clear();
        sentR.clear();

        for (long value : sent) {
            if (value > 0) {
                sentL.add(value);
            } else {
                sentR.add(value);
                partitionb.add(partitioner2.getPartition(value));
            }
        }
    }

    @Override
    public int numPartitions() {
        return partitioner1.getNumPartitions();
    }

    @Override
    public SkipGramPartitioner getPartitioner1() {
        return partitioner1;
    }

    @Override
    public SkipGramPartitioner getPartitioner2() {
        return partitioner2;
    }

    private boolean findNext() {
        while (i < sentL.size() && next == null) {
            if (a == -1) {
                a = partitioner1.getPartition(sentL.getLong(i));
            }
            int n = Math.min(2 * window, sentR.size() - 1);

            while (j < n) {
                int c = i;
                while (c == i) {
                    c = random.nextInt(sentR.size());
                }

                j += 1;
                if (!PairGenerator.skipPair(sentL.getLong(i), sentR.getLong(c), samplingMode) && partitionb.getInt(c) == a) {
                    next = new LongPair(a, sentL.getLong(i), sentR.getLong(c));
                    return true;
                }
            }

            i += 1;
            j = 0;
            a = -1;
        }
        return next != null;
    }

    @Override
    public boolean hasNext() {
        return findNext();
    }

    @Override
    public LongPair next() {
        findNext();
        LongPair r = next;
        next = null;
        return r;
    }
}
