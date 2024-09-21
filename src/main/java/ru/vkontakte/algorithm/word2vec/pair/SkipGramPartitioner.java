package ru.vkontakte.algorithm.word2vec.pair;

import ru.vkontakte.algorithm.word2vec.SkipGramUtil;

import java.io.Serializable;
import java.util.stream.IntStream;

/**
 * @author ezamyatin
 **/
public interface SkipGramPartitioner extends Serializable {
    final static int PART_TABLE_TOTAL_SIZE = 10000000;

    static int[][] createPartitionTable(int numPartitions,
                                        java.util.Random rnd) {
        int nBuckets = PART_TABLE_TOTAL_SIZE / numPartitions;
        int[][] result = new int[nBuckets][numPartitions];
        IntStream.range(0, nBuckets).forEach(bucket -> {
            result[bucket] = SkipGramUtil.shuffle(IntStream.range(0, numPartitions).toArray(), rnd);
        });
        return result;
    }

    static int hash(long i, int salt, int n) {
        long h = (((long)Long.valueOf(i).hashCode()) << 32) | salt;
        h ^= h >>> 33;
        h *= 0xff51afd7ed558ccdL;
        h ^= h >>> 33;
        h *= 0xc4ceb9fe1a85ec53L;
        h ^= h >>> 33;
        return (int)(Math.abs(h) % n);
    }

    int getPartition(long item);

    int getNumPartitions();
}
