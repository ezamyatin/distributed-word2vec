package ru.vkontakte.algorithm.word2vec.pair.generator;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import ru.vkontakte.algorithm.word2vec.SkipGramUtil;
import ru.vkontakte.algorithm.word2vec.pair.LongPair;
import ru.vkontakte.algorithm.word2vec.pair.SamplingMode;
import ru.vkontakte.algorithm.word2vec.pair.SkipGramPartitioner;

import java.util.Iterator;
import java.util.Random;

/**
 * @author ezamyatin
 **/

public class SampleGenerator implements PairGenerator {
    private final int window;
    private final SamplingMode samplingMode;
    private final Random random;

    public SampleGenerator(int window,
                           SamplingMode samplingMode,
                           long seed) {
        this.window = window;
        this.samplingMode = samplingMode;
        this.random = new Random(seed);
    }

    public Iterator<LongPair> generate(long[] sent) {
        return SkipGramUtil.untilNull(new Iterator<LongPair>() {
            private int i = 0;
            private int j = 0;

            @Override
            public boolean hasNext() {
                return true;
            }

            @Override
            public LongPair next() {
                while (i < sent.length) {
                    int n = Math.min(2 * window, sent.length - 1);

                    while (j < n) {
                        int c = i;
                        while (c == i) {
                            c = random.nextInt(sent.length);
                        }

                        j += 1;
                        if (!skipPair(sent, i, c, samplingMode)) {
                            return new LongPair(sent[i], sent[c]);
                        }
                    }

                    i += 1;
                    j = 0;
                }
                return null;
            }
        });
    }
}
