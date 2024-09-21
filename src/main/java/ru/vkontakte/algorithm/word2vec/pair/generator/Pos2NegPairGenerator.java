package ru.vkontakte.algorithm.word2vec.pair.generator;

import com.google.common.collect.Iterators;
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
public class Pos2NegPairGenerator implements PairGenerator {
    private final int window;
    private final SamplingMode samplingMode;
    private final Random random;

    private final IntArrayList sentL, sentR;

    public Pos2NegPairGenerator(int window,
                                SamplingMode samplingMode,
                                long seed) {
        this.window = window;
        this.samplingMode = samplingMode;
        this.random = new Random(seed);

        this.sentL = new IntArrayList(1000);
        this.sentR = new IntArrayList(1000);
    }

    public Iterator<LongPair> generate(long[] sent) {

        sentL.clear();
        sentR.clear();

        for (int i = 0; i < sent.length; ++i) {
            if (sent[i] > 0) {
                sentL.add(i);
            } else {
                sentR.add(i);
            }
        }

        return SkipGramUtil.untilNull(new Iterator<LongPair>() {
            private int i = 0;
            private int j = 0;

            @Override
            public boolean hasNext() {
                return true;
            }

            @Override
            public LongPair next() {
                while (i < sentL.size()) {
                    int n = Math.min(2 * window, sentR.size() - 1);

                    while (j < n) {
                        int c = random.nextInt(sentR.size());;

                        j += 1;
                        if (!skipPair(sent, sentL.getInt(i), sentR.getInt(c), samplingMode)) {
                            return new LongPair(sent[sentL.getInt(i)], sent[sentR.getInt(c)]);
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
