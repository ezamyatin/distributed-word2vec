package ru.vkontakte.mf.local;


import java.io.Serializable;

/**
 * @author ezamyatin
 **/
public class Opts implements Serializable {
    public final int dim;
    public final boolean useBias;
    public final int negative;
    public final double pow;
    public final double lr;
    public final double lambda;
    public final float gamma;

    public Opts(int dim, boolean useBias, int negative, double pow, double lr, double lambda, float gamma) {
        this.dim = dim;
        this.useBias = useBias;
        this.negative = negative;
        this.pow = pow;
        this.lr = lr;
        this.lambda = lambda;
        this.gamma = gamma;
    }

    public int vectorSize() {
        return useBias ? dim + 1 : dim;
    }
}
