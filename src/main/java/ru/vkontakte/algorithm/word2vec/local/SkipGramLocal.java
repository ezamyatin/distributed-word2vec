package ru.vkontakte.algorithm.word2vec.local;

import com.google.common.collect.Iterators;
import com.google.common.util.concurrent.AtomicDouble;
import it.unimi.dsi.fastutil.floats.FloatArrayList;
import it.unimi.dsi.fastutil.longs.Long2IntOpenHashMap;
import com.github.fommil.netlib.BLAS;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import ru.vkontakte.algorithm.word2vec.pair.LongPairMulti;
import ru.vkontakte.algorithm.word2vec.pair.generator.w2v.SamplingMode;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongConsumer;

/**
 * @author ezamyatin
 **/
public class SkipGramLocal {
    private final static int UNIGRAM_TABLE_SIZE = 100000000;

    private static class ExpTable {
        final public static int EXP_TABLE_SIZE = 1000;
        final public static int MAX_EXP = 6;

        public float[] sigm;
        public float[] loss0;
        public float[] loss1;

        private static final ExpTable INSTANCE = new ExpTable();

        private ExpTable() {
            sigm = new float[EXP_TABLE_SIZE];
            loss0 = new float[EXP_TABLE_SIZE];
            loss1 = new float[EXP_TABLE_SIZE];

            int i = 0;
            while (i < EXP_TABLE_SIZE) {
                double tmp = Math.exp((2.0 * i / EXP_TABLE_SIZE - 1.0) * MAX_EXP);
                sigm[i] = (float)(tmp / (tmp + 1.0));
                loss0[i] = (float)Math.log(sigm[i]);
                loss1[i] = (float)Math.log(1 - sigm[i]);
                i += 1;
            }
        }

        public static ExpTable getInstance() {
            return INSTANCE;
        }
    }

    private final SkipGramOpts opts;

    private final Long2IntOpenHashMap vocabL, vocabR;
    private final long[] i2R;
    private final long[] cnL, cnR;
    private final float[] syn0, syn1neg;
    private final int[] unigramTable;
    private final ThreadLocalRandom random;
    private final BLAS blas = BLAS.getInstance();

    public final AtomicDouble loss;
    public final AtomicLong lossn;

    private static int[] initUnigramTable(long[] cn, double pow) {
        int[] table = new int[UNIGRAM_TABLE_SIZE];

        int n = cn.length;

        int a = 0;
        double trainWordsPow = 0.0;

        while (a < n) {
            trainWordsPow += Math.pow(cn[a], pow);
            a += 1;
        }

        int i = 0;
        a = 0;
        double d1 = Math.pow(cn[i], pow) / trainWordsPow;

        while (a < table.length && i < n) {
            table[a] = i;
            if (a > d1 * table.length) {
                i += 1;
                d1 += Math.pow(cn[i], pow) / trainWordsPow;
            }
            a += 1;
        }

        return table;
    }

    public SkipGramLocal(SkipGramOpts opts, Iterator<ItemData> iterator) {
        this.opts = opts;
        this.vocabL = new Long2IntOpenHashMap();
        this.vocabR = new Long2IntOpenHashMap();

        LongArrayList cnL = new LongArrayList();
        LongArrayList cnR = new LongArrayList();

        FloatArrayList rawSyn0 = new FloatArrayList();
        FloatArrayList rawSyn1neg = new FloatArrayList();

        while (iterator.hasNext()) {
            ItemData itemData = iterator.next();

            if (itemData.type == ItemData.TYPE_LEFT) {
                int i = vocabL.size();
                vocabL.put(itemData.id, i);
                cnL.add(itemData.cn);
                for (float v : itemData.f) {
                    rawSyn0.add(v);
                }
            } else {
                int i = vocabR.size();
                vocabR.put(itemData.id, i);
                cnR.add(itemData.cn);
                for (float v : itemData.f) {
                    rawSyn1neg.add(v);
                }
            }
        }

        this.i2R = new long[vocabR.size()];
        vocabR.keySet().forEach((LongConsumer)e -> i2R[vocabR.get(e)] = e);

        this.cnL = cnL.toLongArray();
        cnL = null;
        this.cnR = cnR.toLongArray();
        cnR = null;

        if (opts.pow > 0) {
            unigramTable = initUnigramTable(this.cnR, opts.pow);
        } else {
            unigramTable = null;
        }
        syn0 = rawSyn0.toFloatArray();
        rawSyn0 = null;
        syn1neg = rawSyn1neg.toFloatArray();
        rawSyn1neg = null;

        random = ThreadLocalRandom.current();
        loss = new AtomicDouble(0);
        lossn = new AtomicLong(0);
    }

    public static float[] initEmbedding(int dim, boolean useBias, Random rnd) {
        float[] f = new float[useBias ? dim + 1: dim];
        for (int i = 0; i < dim; i++) {
            f[i] = (rnd.nextFloat() - 0.5f) / dim;
        }
        return f;
    }

    private static void shuffle(long[] l, long[] r, @Nullable float[] w, Random rnd) {
        int i = 0;
        int n = l.length;
        long t;
        float t1;

        while (i < n - 1) {
            int j = i + rnd.nextInt(n - i);
            t = l[j];
            l[j] = l[i];
            l[i] = t;

            t = r[j];
            r[j] = r[i];
            r[i] = t;

            if (w != null) {
                t1 = w[j];
                w[j] = w[i];
                w[i] = t1;
            }

            i += 1;
        }
    }

    private void optimizeBatchRemapped(long[] l, long[] r, @Nullable float[] w) {
        assert l.length == r.length;
        shuffle(l, r, w, random);

        double lloss = 0.0;
        long llossn = 0L;
        int pos = 0;
        int word;
        int lastWord;
        float[] neu1e = new float[opts.vectorSize()];
        ExpTable expTable = ExpTable.getInstance();

        while (pos < l.length) {
            lastWord = (int)l[pos];
            word = (int)r[pos];

            if (word != -1 && lastWord != -1) {
                int l1 = lastWord * opts.vectorSize();
                Arrays.fill(neu1e, 0);
                int target;
                int label;
                float weight;
                int d = 0;

                while (d < opts.negative + 1) {
                    if (d == 0) {
                        target = word;
                        label = 1;
                        weight = w == null ? 1f : w[pos];
                    } else {
                        if (unigramTable != null) {
                            target = unigramTable[random.nextInt(unigramTable.length)];
                            while (target == -1 || l[pos] == i2R[target]) {
                                target = unigramTable[random.nextInt(unigramTable.length)];
                            }
                        } else {
                            target = random.nextInt(vocabR.size());
                            while (l[pos] == i2R[target]) {
                                target = random.nextInt(vocabR.size());
                            }
                        }
                        weight = 1f;
                        label = 0;
                    }
                    int l2 = target * opts.vectorSize();
                    float f = blas.sdot(opts.dim, syn0, l1, 1, syn1neg, l2, 1);
                    if (opts.useBias) {
                        f += syn0[l1 + opts.dim];
                        f += syn1neg[l2 + opts.dim];
                    }

                    float g;
                    float sigm;

                    if (f > ExpTable.MAX_EXP) {
                        sigm = 1.0f;
                        lloss += (-(label > 0 ? 0 : -6.00247569));
                        llossn += 1;
                    } else if (f < -ExpTable.MAX_EXP) {
                        sigm = 0.0f;
                        lloss += (-(label > 0 ? -6.00247569 : 0));
                        llossn += 1;
                    } else {
                        int ind = (int)((f + ExpTable.MAX_EXP) * (ExpTable.EXP_TABLE_SIZE / ExpTable.MAX_EXP / 2.0));
                        sigm = expTable.sigm[ind];
                        lloss += (-((label > 0) ? expTable.loss1[ind] : expTable.loss0[ind]));
                        llossn += 1;
                    }
                    g = (float)((label - sigm) * opts.lr * weight);

                    if (opts.lambda > 0) {
                        blas.saxpy(opts.dim, (float)(-opts.lambda * opts.lr), syn0, l1, 1, neu1e, 0, 1);
                    }
                    blas.saxpy(opts.dim, g, syn1neg, l2, 1, neu1e, 0, 1);
                    if (opts.useBias) {
                        neu1e[opts.dim] += g * 1;
                    }

                    if (opts.lambda > 0) {
                        blas.saxpy(opts.dim, (float)(-opts.lambda * opts.lr), syn1neg, l2, 1, syn1neg, l2, 1);
                    }
                    blas.saxpy(opts.dim, g, syn0, l1, 1, syn1neg, l2, 1);
                    if (opts.useBias) {
                        syn1neg[l2 + opts.dim] += g * 1;
                    }
                    d += 1;
                }
                blas.saxpy(opts.vectorSize(), 1.0f, neu1e, 0, 1, syn0, l1, 1);
            }
            pos += 1;
        }

        loss.addAndGet(lloss);
        lossn.addAndGet(llossn);
    }

    public void optimize(Iterator<LongPairMulti> data, int cpus) {
        ParItr.foreach(new Iterator<LongPairMulti>() {
            @Override
            public boolean hasNext() {
                return data.hasNext();
            }

            @Override
            public LongPairMulti next() {
                return data.next().remap(vocabL, vocabR);
            }
        }, t -> this.optimizeBatchRemapped(t.left, t.right, t.rating), cpus);
    }

    public Iterator<ItemData> flush() {
        return Iterators.concat(
                vocabL.keySet().longStream().mapToObj(e -> {
                    int i = vocabL.get(e);
                    return new ItemData(ItemData.TYPE_LEFT, e, cnL[i],
                            Arrays.copyOfRange(syn0, opts.vectorSize() * i, opts.vectorSize() * (i + 1)));

                }).iterator(),
                vocabR.keySet().longStream().mapToObj(e -> {
                    int i = vocabR.get(e);
                    return new ItemData(ItemData.TYPE_RIGHT, e, cnR[i],
                            Arrays.copyOfRange(syn1neg, opts.vectorSize() * i, opts.vectorSize() * (i + 1)));

                }).iterator());
    }

}
