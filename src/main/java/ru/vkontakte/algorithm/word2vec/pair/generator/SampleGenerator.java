package ru.vkontakte.algorithm.word2vec.pair.generator;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import ru.vkontakte.algorithm.word2vec.pair.LongPair;
import ru.vkontakte.algorithm.word2vec.pair.SamplingMode;
import ru.vkontakte.algorithm.word2vec.pair.SkipGramPartitioner;

import java.util.Random;

/**
 * @author ezamyatin
 **/
public class SampleGenerator implements PairGenerator {
    private final int window;
    private final SkipGramPartitioner partitioner1;
    private final SkipGramPartitioner partitioner2;
    private final SamplingMode samplingMode;
    private final Random random;

    private long[] sent = null;
    private LongPair next = null;
    private int i = 0;
    private int j = 0;
    private int a = -1;

    private final IntArrayList partitionb;

    public SampleGenerator(int window,
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
        this.sent = new long[0];
    }

    public void reset(long[] sent) {
        assert next == null;

        this.sent = sent;
        next = null;
        i = 0;
        j = 0;
        a = -1;
        partitionb.clear();

        for (long value : sent) {
            partitionb.add(partitioner2.getPartition(value));
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
        while (i < sent.length && next == null) {
            if (a == -1) {
                a = partitioner1.getPartition(sent[i]);
            }
            int n = Math.min(2 * window, sent.length - 1);

            while (j < n) {
                int c = i;
                while (c == i) {
                    c = random.nextInt(sent.length);
                }

                j += 1;
                if (!PairGenerator.skipPair(sent[c], sent[i], samplingMode) && partitionb.getInt(c) == a) {
                    next = new LongPair(a, sent[i], sent[c]);
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
