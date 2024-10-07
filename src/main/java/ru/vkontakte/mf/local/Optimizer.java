package ru.vkontakte.mf.local;

import com.google.common.collect.Iterators;
import com.google.common.util.concurrent.AtomicDouble;
import it.unimi.dsi.fastutil.floats.FloatArrayList;
import it.unimi.dsi.fastutil.longs.Long2IntOpenHashMap;
import com.github.fommil.netlib.BLAS;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import ru.vkontakte.mf.pair.LongPairMulti;

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
public class Optimizer {
    private final static int UNIGRAM_TABLE_SIZE = 100000000;

    private static class ExpTable {
        final public static int EXP_TABLE_SIZE = 1000;
        final public static int MAX_EXP = 6;

        private final float[] sigm;
        private final float[] loss0;
        private final float[] loss1;

        private static final ExpTable INSTANCE = new ExpTable();

        private ExpTable() {
            sigm = new float[EXP_TABLE_SIZE];
            loss0 = new float[EXP_TABLE_SIZE];
            loss1 = new float[EXP_TABLE_SIZE];

            int i = 0;
            while (i < EXP_TABLE_SIZE) {
                double tmp = Math.exp((2.0 * i / EXP_TABLE_SIZE - 1.0) * MAX_EXP);
                sigm[i] = (float)(tmp / (tmp + 1.0));
                loss0[i] = (float)Math.log(1 - sigm[i]);
                loss1[i] = (float)Math.log(sigm[i]);
                i += 1;
            }
        }

        public static ExpTable getInstance() {
            return INSTANCE;
        }

        public float sigmoid(float f) {
            if (f > ExpTable.MAX_EXP) {
                return 1.0f;
            } else if (f < -ExpTable.MAX_EXP) {
                return 0.0f;
            } else {
                int ind = (int)((f + ExpTable.MAX_EXP) * (ExpTable.EXP_TABLE_SIZE / ExpTable.MAX_EXP / 2.0));
                return this.sigm[ind];
            }
        }

        public float logloss(float f, float label) {
            if (f > ExpTable.MAX_EXP) {
                return (-(label > 0 ? 0f : -6.00247569f));
            } else if (f < -ExpTable.MAX_EXP) {
                return  (-(label > 0 ? -6.00247569f : 0f));
            } else {
                int ind = (int)((f + ExpTable.MAX_EXP) * (ExpTable.EXP_TABLE_SIZE / ExpTable.MAX_EXP / 2.0));
                return (-((label > 0) ? this.loss1[ind] : this.loss0[ind]));
            }
        }
    }

    private final Opts opts;

    private final Long2IntOpenHashMap vocabL, vocabR;
    private final long[] i2R;
    private final long[] cnL, cnR;
    private final float[] syn0, syn1neg;
    private final int[] unigramTable;
    private final ThreadLocalRandom random;
    private final BLAS blas = BLAS.getInstance();

    public final AtomicDouble loss, lossReg;
    public final AtomicLong lossn, lossnReg;

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

    public Optimizer(Opts opts, Iterator<ItemData> iterator) {
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

        this.cnL = cnL.toLongArray();
        cnL = null;
        this.cnR = cnR.toLongArray();
        cnR = null;

        if (opts.implicit) {
            this.i2R = new long[vocabR.size()];
            vocabR.keySet().forEach((LongConsumer) e -> i2R[vocabR.get(e)] = e);

            if (opts.pow > 0) {
                unigramTable = initUnigramTable(this.cnR, opts.pow);
            } else {
                unigramTable = null;
            }

        } else {
            this.i2R = null;
            unigramTable = null;
        }

        syn0 = rawSyn0.toFloatArray();
        rawSyn0 = null;
        syn1neg = rawSyn1neg.toFloatArray();
        rawSyn1neg = null;

        random = ThreadLocalRandom.current();
        loss = new AtomicDouble(0);
        lossReg = new AtomicDouble(0);
        lossn = new AtomicLong(0);
        lossnReg = new AtomicLong(0);
    }

    public static float[] initEmbedding(int dim, boolean useBias, Random rnd) {
        float[] f = new float[useBias ? dim + 1: dim];
        for (int i = 0; i < dim; i++) {
            f[i] = (rnd.nextFloat() - 0.5f) / dim;
        }
        return f;
    }

    private static void shuffle(LongPairMulti batch, Random rnd) {
        int i = 0;
        int n = batch.label.length;
        long t;
        float t1;

        while (i < n - 1) {
            int j = i + rnd.nextInt(n - i);
            t = batch.left[j];
            batch.left[j] = batch.left[i];
            batch.left[i] = t;

            t = batch.right[j];
            batch.right[j] = batch.right[i];
            batch.right[i] = t;

            if (batch.label != null) {
                t1 = batch.label[j];
                batch.label[j] = batch.label[i];
                batch.label[i] = t1;
            }

            if (batch.weight != null) {
                t1 = batch.weight[j];
                batch.weight[j] = batch.weight[i];
                batch.weight[i] = t1;
            }

            i += 1;
        }
    }

    private void optimizeImplicitBatchRemapped(LongPairMulti batch) {
        assert batch.left.length == batch.right.length;
        assert batch.label == null;

        shuffle(batch, random);

        double lloss = 0.0;
        double llossReg = 0.0;
        long llossn = 0L;
        long llossnReg = 0L;
        int pos = 0;
        int word;
        int lastWord;
        float[] neu1e = new float[opts.vectorSize()];
        ExpTable expTable = ExpTable.getInstance();

        while (pos < batch.left.length) {
            lastWord = (int)batch.left[pos];
            word = (int)batch.right[pos];

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
                        weight = batch.weight == null ? 1f : batch.weight[pos];
                    } else {
                        if (unigramTable != null) {
                            target = unigramTable[random.nextInt(unigramTable.length)];
                            while (target == -1 || batch.left[pos] == i2R[target]) {
                                target = unigramTable[random.nextInt(unigramTable.length)];
                            }
                        } else {
                            target = random.nextInt(vocabR.size());
                            while (batch.left[pos] == i2R[target]) {
                                target = random.nextInt(vocabR.size());
                            }
                        }
                        weight = batch.weight == null ? opts.gamma : (batch.weight[pos] * opts.gamma);
                        label = 0;
                    }
                    int l2 = target * opts.vectorSize();
                    float f = blas.sdot(opts.dim, syn0, l1, 1, syn1neg, l2, 1);
                    if (opts.useBias) {
                        f += syn0[l1 + opts.dim];
                        f += syn1neg[l2 + opts.dim];
                    }

                    float sigm = expTable.sigmoid(f);
                    float g = (float)((label - sigm) * opts.lr * weight);

                    if (opts.verbose) {
                        lloss += expTable.logloss(f, label) * weight;
                        llossn += 1;

                        if (opts.lambda > 0 && label > 0) {
                            llossReg += opts.lambda * blas.sdot(opts.dim, syn0, l1, 1, syn0, l1, 1);
                            llossReg += opts.lambda * blas.sdot(opts.dim, syn1neg, l2, 1, syn1neg, l2, 1);
                            llossnReg += 1;
                        }
                    }

                    if (opts.lambda > 0 && label > 0) {
                        blas.saxpy(opts.dim, (float)(-opts.lambda * opts.lr), syn0, l1, 1, neu1e, 0, 1);
                    }
                    blas.saxpy(opts.dim, g, syn1neg, l2, 1, neu1e, 0, 1);
                    if (opts.useBias) {
                        neu1e[opts.dim] += g * 1;
                    }

                    if (opts.lambda > 0 && label > 0) {
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
        lossReg.addAndGet(llossReg);
        lossn.addAndGet(llossn);
        lossnReg.addAndGet(llossnReg);
    }

    private void optimizeExplicitBatchRemapped(LongPairMulti batch) {
        assert batch.left.length == batch.right.length;
        assert Float.isNaN(opts.gamma);

        shuffle(batch, random);

        double lloss = 0.0;
        double llossReg = 0.0;
        long llossn = 0L;
        long llossnReg = 0L;
        int pos = 0;
        int word;
        int lastWord;
        float[] neu1e = new float[opts.vectorSize()];
        ExpTable expTable = ExpTable.getInstance();

        while (pos < batch.left.length) {
            lastWord = (int)batch.left[pos];
            word = (int)batch.right[pos];

            if (word != -1 && lastWord != -1) {
                Arrays.fill(neu1e, 0);
                final int l1 = lastWord * opts.vectorSize();
                final int l2 = word * opts.vectorSize();
                final float label = batch.label[pos];
                final float weight = batch.weight == null ? 1f : batch.weight[pos];

                assert label == 0.0 || label == 1.0;

                float f = blas.sdot(opts.dim, syn0, l1, 1, syn1neg, l2, 1);
                if (opts.useBias) {
                    f += syn0[l1 + opts.dim];
                    f += syn1neg[l2 + opts.dim];
                }

                float sigm = expTable.sigmoid(f);
                float g = (float)((label - sigm) * opts.lr * weight);

                if (opts.verbose) {
                    lloss += expTable.logloss(f, label) * weight;
                    llossn += 1;

                    if (opts.lambda > 0 && label > 0) {
                        llossReg += opts.lambda * blas.sdot(opts.dim, syn0, l1, 1, syn0, l1, 1);
                        llossReg += opts.lambda * blas.sdot(opts.dim, syn1neg, l2, 1, syn1neg, l2, 1);
                        llossnReg += 1;
                    }
                }

                if (opts.lambda > 0 && label > 0) {
                    blas.saxpy(opts.dim, (float)(-opts.lambda * opts.lr), syn0, l1, 1, neu1e, 0, 1);
                }
                blas.saxpy(opts.dim, g, syn1neg, l2, 1, neu1e, 0, 1);
                if (opts.useBias) {
                    neu1e[opts.dim] += g * 1;
                }

                if (opts.lambda > 0 && label > 0) {
                    blas.saxpy(opts.dim, (float)(-opts.lambda * opts.lr), syn1neg, l2, 1, syn1neg, l2, 1);
                }
                blas.saxpy(opts.dim, g, syn0, l1, 1, syn1neg, l2, 1);
                if (opts.useBias) {
                    syn1neg[l2 + opts.dim] += g * 1;
                }
                blas.saxpy(opts.vectorSize(), 1.0f, neu1e, 0, 1, syn0, l1, 1);
            }
            pos += 1;
        }

        loss.addAndGet(lloss);
        lossReg.addAndGet(llossReg);
        lossn.addAndGet(llossn);
        lossnReg.addAndGet(llossnReg);
    }

    public void optimize(Iterator<LongPairMulti> data, int cpus) {
        Iterator<LongPairMulti> remapped = new Iterator<LongPairMulti>() {
            @Override
            public boolean hasNext() {
                return data.hasNext();
            }

            @Override
            public LongPairMulti next() {
                return data.next().remap(vocabL, vocabR);
            }
        };

        if (cpus == 1) {
            remapped.forEachRemaining(opts.implicit ? this::optimizeImplicitBatchRemapped : this::optimizeExplicitBatchRemapped);
        } else {
            ParItr.foreach(remapped, cpus, opts.implicit ? this::optimizeImplicitBatchRemapped : this::optimizeExplicitBatchRemapped);
        }
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
