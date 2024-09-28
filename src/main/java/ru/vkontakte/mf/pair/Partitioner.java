package ru.vkontakte.mf.pair;

import java.io.Serializable;
import java.util.Random;
import java.util.stream.IntStream;

/**
 * @author ezamyatin
 **/
public abstract class Partitioner implements Serializable {
    final static int PART_TABLE_TOTAL_SIZE = 10000000;

    private static int[] shuffle(int[] arr, Random rnd) {
        int i = 0;
        int n = arr.length;
        int t;

        while (i < n - 1) {
            int j = i + rnd.nextInt(n - i);
            t = arr[j];
            arr[j] = arr[i];
            arr[i] = t;

            i += 1;
        }

        return arr;
    }

    public static int[][] createPartitionTable(int numPartitions, java.util.Random rnd) {
        int nBuckets = PART_TABLE_TOTAL_SIZE / numPartitions;
        int[][] result = new int[nBuckets][numPartitions];
        IntStream.range(0, nBuckets).forEach(bucket -> {
            result[bucket] = shuffle(IntStream.range(0, numPartitions).toArray(), rnd);
        });
        return result;
    }

    public static int hash(long i, int salt, int n) {
        long h = (((long)Long.valueOf(i).hashCode()) << 32) | salt;
        h ^= h >>> 33;
        h *= 0xff51afd7ed558ccdL;
        h ^= h >>> 33;
        h *= 0xc4ceb9fe1a85ec53L;
        h ^= h >>> 33;
        return (int)(Math.abs(h) % n);
    }

    public abstract int getPartition(long item);

    public abstract int getNumPartitions();
}
