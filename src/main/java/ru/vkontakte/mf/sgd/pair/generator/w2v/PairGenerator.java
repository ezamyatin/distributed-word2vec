package ru.vkontakte.mf.sgd.pair.generator.w2v;

import com.google.common.collect.Iterators;
import ru.vkontakte.mf.sgd.pair.LongPair;
import ru.vkontakte.mf.sgd.pair.Partitioner;
import ru.vkontakte.mf.sgd.pair.generator.UntilNullIterator;

import java.io.Serializable;
import java.util.Iterator;

/**
 * @author ezamyatin
 **/

public abstract class PairGenerator extends UntilNullIterator<LongPair> implements Serializable {

    private final Iterator<long[]> sent;
    private final Partitioner partitioner1;
    private final Partitioner partitioner2;

    private Iterator<LongPair> it = Iterators.emptyIterator();

    protected abstract Iterator<LongPair> generate(long[] sent);

    public Partitioner partitioner1() {
        return partitioner1;
    }

    public Partitioner partitioner2(){
        return partitioner2;
    }

    public PairGenerator(Iterator<long[]> sent,
                         Partitioner partitioner1,
                         Partitioner partitioner2) {
        assert partitioner1.numPartitions() == partitioner2.numPartitions();
        this.sent = sent;
        this.partitioner1 = partitioner1;
        this.partitioner2 = partitioner2;
    }

    public LongPair generateOrNull() {
        while (!it.hasNext() && sent.hasNext()) {
            it = generate(sent.next());
        }
        if (it.hasNext()) {
            return it.next();
        }
        return null;
    }
}
