package ru.vkontakte.mf.pair.generator.w2v;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import ru.vkontakte.mf.pair.LongPair;
import ru.vkontakte.mf.pair.Partitioner;
import ru.vkontakte.mf.pair.generator.UntilNullIterator;

import java.util.Iterator;
import java.util.Random;

/**
 * @author ezamyatin
 **/
public class WindowGenerator extends PairGenerator {
    private final int window;
    private final IntArrayList p1, p2;

    public WindowGenerator(Iterator<long[]> sent,
                           int window,
                           Partitioner partitioner1,
                           Partitioner partitioner2) {
        super(sent, partitioner1, partitioner2);

        this.window = window;
        this.p1 = new IntArrayList(1000);
        this.p2 = new IntArrayList(1000);
    }

    protected Iterator<LongPair> generate(long[] sent) {

        p1.clear();
        p2.clear();

        for (int i = 0; i < sent.length; ++i) {
            p1.add(partitioner1().getPartition(sent[i]));
            p2.add(partitioner2().getPartition(sent[i]));
        }

        return new UntilNullIterator<LongPair>() {
            private int i = 0;
            private int j = -window;

            @Override
            public LongPair generateOrNull() {
                while (i < sent.length) {
                    j = Math.max(j, -i);

                    while (j <= window && i + j < sent.length) {
                        int c = i + j;
                        j += 1;
                        if (p1.getInt(i) == p2.getInt(c) && sent[i] != sent[c]) {
                            return new LongPair(p1.getInt(i), sent[i], sent[c]);
                        }
                    }

                    i += 1;
                    j = -window;
                }
                return null;
            }
        };
    }
}
