package ru.vkontakte.algorithm.word2vec.local;


import ru.vkontakte.algorithm.word2vec.pair.SamplingMode;

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
    public final SamplingMode samplingMode;

    public SkipGramOpts(int dim, boolean useBias, int negative, int window, double pow, double lr,
                        double lambda, SamplingMode samplingMode) {
        this.dim = dim;
        this.useBias = useBias;
        this.negative = negative;
        this.window = window;
        this.pow = pow;
        this.lr = lr;
        this.lambda = lambda;
        this.samplingMode = samplingMode;
    }

    public SkipGramOpts copy(double newlr) {
        return new SkipGramOpts(dim, useBias, negative, window,
                pow, newlr, lambda, samplingMode);
    }

    public int vectorSize() {
        return useBias ? dim + 1 : dim;
    }
}
