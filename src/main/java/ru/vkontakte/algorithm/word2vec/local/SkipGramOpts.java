package ru.vkontakte.algorithm.word2vec.local;


import ru.vkontakte.algorithm.word2vec.pair.generator.w2v.SamplingMode;

import java.io.Serializable;

/**
 * @author ezamyatin
 **/
public class SkipGramOpts implements Serializable {
    public final int dim;
    public final boolean useBias;
    public final int negative;
    public final int window;
    public final double pow;
    public final double lr;
    public final double lambda;

    public SkipGramOpts(int dim, boolean useBias, int negative, int window, double pow, double lr, double lambda) {
        this.dim = dim;
        this.useBias = useBias;
        this.negative = negative;
        this.window = window;
        this.pow = pow;
        this.lr = lr;
        this.lambda = lambda;
    }

    public int vectorSize() {
        return useBias ? dim + 1 : dim;
    }
}
