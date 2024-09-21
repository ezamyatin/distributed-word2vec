package ru.vkontakte.algorithm.word2vec.pair.generator;

import ru.vkontakte.algorithm.word2vec.pair.LongPair;
import ru.vkontakte.algorithm.word2vec.pair.SamplingMode;
import ru.vkontakte.algorithm.word2vec.pair.SkipGramPartitioner;

import java.io.Serializable;
import java.util.Iterator;

/**
 * @author ezamyatin
 **/

public interface PairGenerator extends Iterator<LongPair>, Serializable {
    static boolean skipPair(long i, long j, SamplingMode samplingMode) {
        return i == j;
    }

    void reset(long[] sent);

    int numPartitions();

    SkipGramPartitioner getPartitioner1();
    SkipGramPartitioner getPartitioner2();
}
