package ru.vkontakte.algorithm.word2vec.pair.generator;

import ru.vkontakte.algorithm.word2vec.pair.LongPair;
import ru.vkontakte.algorithm.word2vec.pair.SamplingMode;
import ru.vkontakte.algorithm.word2vec.pair.SkipGramPartitioner;

import java.io.Serializable;
import java.util.Iterator;

/**
 * @author ezamyatin
 **/

public interface PairGenerator extends Serializable {

    Iterator<LongPair> generate(long[] sent);

    SkipGramPartitioner partitioner1();
    SkipGramPartitioner partitioner2();
}
