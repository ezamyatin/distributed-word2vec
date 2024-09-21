package ru.vkontakte.algorithm.word2vec.pair.generator;

import ru.vkontakte.algorithm.word2vec.SkipGramUtil;
import ru.vkontakte.algorithm.word2vec.pair.LongPair;
import ru.vkontakte.algorithm.word2vec.pair.SamplingMode;

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
        return new UntilNullIterator<LongPair>() {
            private int i = 0;
            private int j = 0;

            @Override
            public LongPair generateOrNull() {
                while (i < sent.length) {
                    int n = Math.min(2 * window, sent.length - 1);

                    while (j < n) {
                        int c = i;
                        while (c == i) {
                            c = random.nextInt(sent.length);
                        }

                        j += 1;
                        if (acceptPair(sent, i, c, samplingMode)) {
                            return new LongPair(sent[i], sent[c]);
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
