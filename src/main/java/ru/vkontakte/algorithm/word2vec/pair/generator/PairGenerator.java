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

    default boolean skipPair(long[] sent, int i, int j, SamplingMode samplingMode) {
        return sent[i] == sent[j];
    }

    Iterator<LongPair> generate(long[] sent);
}
