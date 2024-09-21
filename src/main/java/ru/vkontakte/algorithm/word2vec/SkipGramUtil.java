package ru.vkontakte.algorithm.word2vec;

import java.util.Random;

/**
 * @author ezamyatin
 **/
public class SkipGramUtil {
    public static void shuffle(long[] l, long[] r, Random rnd) {
        int i = 0;
        int n = l.length;
        long t;

        while (i < n - 1) {
            int j = i + rnd.nextInt(n - i);
            t = l[j];
            l[j] = l[i];
            l[i] = t;

            t = r[j];
            r[j] = r[i];
            r[i] = t;

            i += 1;
        }
    }

    public static int[] shuffle(int[] arr, Random rnd) {
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
}
