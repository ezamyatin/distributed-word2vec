package ru.vkontakte.mf.pair.generator.w2v;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import ru.vkontakte.mf.pair.Partitioner;
import ru.vkontakte.mf.pair.generator.UntilNullIterator;
import ru.vkontakte.mf.pair.LongPair;

import java.util.Iterator;
import java.util.Random;

/**
 * @author ezamyatin
 **/
public class Pos2NegGenerator extends PairGenerator {
    private final int window;
    private final Random random;

    private final IntArrayList sentL, sentR;
    private final IntArrayList p1, p2;

    public Pos2NegGenerator(Iterator<long[]> sent,
                            int window,
                            Partitioner partitioner1,
                            Partitioner partitioner2,
                            long seed) {
        super(sent, partitioner1, partitioner2);

        this.window = window;
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
                p1.add(partitioner1().getPartition(sent[i]));
            } else {
                sentR.add(i);
                p2.add(partitioner2().getPartition(sent[i]));
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
