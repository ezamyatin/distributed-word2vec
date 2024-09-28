package ru.vkontakte.algorithm.word2vec.pair.generator.w2v;

import com.google.common.collect.Iterators;
import ru.vkontakte.algorithm.word2vec.pair.LongPair;
import ru.vkontakte.algorithm.word2vec.pair.SkipGramPartitioner;
import ru.vkontakte.algorithm.word2vec.pair.generator.UntilNullIterator;

import java.io.Serializable;
import java.util.Iterator;

/**
 * @author ezamyatin
 **/

public abstract class PairGenerator extends UntilNullIterator<LongPair> implements Serializable {

    private final Iterator<long[]> sent;
    private final SkipGramPartitioner partitioner1;
    private final SkipGramPartitioner partitioner2;

    private Iterator<LongPair> it = Iterators.emptyIterator();

    protected abstract Iterator<LongPair> generate(long[] sent);

    public SkipGramPartitioner partitioner1() {
        return partitioner1;
    }

    public SkipGramPartitioner partitioner2(){
        return partitioner2;
    }

    public PairGenerator(Iterator<long[]> sent,
                         SkipGramPartitioner partitioner1,
                         SkipGramPartitioner partitioner2) {
        assert partitioner1.getNumPartitions() == partitioner2.getNumPartitions();
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
