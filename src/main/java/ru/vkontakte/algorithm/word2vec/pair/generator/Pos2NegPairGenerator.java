package ru.vkontakte.algorithm.word2vec.pair.generator;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import ru.vkontakte.algorithm.word2vec.pair.LongPair;
import ru.vkontakte.algorithm.word2vec.pair.SamplingMode;
import ru.vkontakte.algorithm.word2vec.pair.SkipGramPartitioner;

import java.util.Iterator;
import java.util.Random;

/**
 * @author ezamyatin
 **/
public class Pos2NegPairGenerator extends PairGenerator {
    private final int window;
    private final SamplingMode samplingMode;
    private final SkipGramPartitioner partitioner1;
    private final SkipGramPartitioner partitioner2;
    private final Random random;

    private final IntArrayList sentL, sentR;
    private final IntArrayList p1, p2;

    public Pos2NegPairGenerator(Iterator<long[]> sent,
                                int window,
                                SamplingMode samplingMode,
                                SkipGramPartitioner partitioner1,
                                SkipGramPartitioner partitioner2,
                                long seed) {
        super(sent, partitioner1, partitioner2);

        this.window = window;
        this.samplingMode = samplingMode;
        this.partitioner1 = partitioner1;
        this.partitioner2 = partitioner2;
        this.random = new Random(seed);

        this.sentL = new IntArrayList(1000);
        this.sentR = new IntArrayList(1000);

        this.p1 = new IntArrayList(1000);
        this.p2 = new IntArrayList(1000);
    }

    protected Iterator<LongPair> generate(long[] sent) {

        sentL.clear();
        sentR.clear();
        p1.clear();
        p2.clear();

        for (int i = 0; i < sent.length; ++i) {
            if (sent[i] > 0) {
                sentL.add(i);
                p1.add(partitioner1.getPartition(sent[i]));
            } else {
                sentR.add(i);
                p2.add(partitioner2.getPartition(sent[i]));
            }
        }

        return new UntilNullIterator<LongPair>() {
            private int i = 0;
            private int j = 0;

            @Override
            public LongPair generateOrNull() {
                while (i < sentL.size()) {
                    int n = Math.min(2 * window, sentR.size() - 1);

                    while (j < n) {
                        int c = random.nextInt(sentR.size());;

                        j += 1;
                        if (p1.getInt(i) == p2.getInt(c) && sent[sentL.getInt(i)] != sent[sentR.getInt(c)]) {
                            return new LongPair(p1.getInt(i), sent[sentL.getInt(i)], sent[sentR.getInt(c)]);
                        }
                    }

                    i += 1;
                    j = 0;
                }
                return null;
            }
        };
    }
}
