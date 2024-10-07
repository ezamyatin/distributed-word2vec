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
    public final boolean implicit;
    public final boolean verbose;

    private Opts(int dim, boolean useBias, int negative, double pow, double lr, double lambda, float gamma, boolean implicit, boolean verbose) {
        this.dim = dim;
        this.useBias = useBias;
        this.negative = negative;
        this.pow = pow;
        this.lr = lr;
        this.lambda = lambda;
        this.gamma = gamma;
        this.implicit = implicit;
        this.verbose = verbose;
    }

    public static Opts implicit(int dim, boolean useBias, int negative, double pow, double lr, double lambda, float gamma, boolean verbose) {
        return new Opts(dim, useBias, negative, pow, lr, lambda, gamma, true, verbose);
    }

    public static Opts explicit(int dim, boolean useBias, int negative, double pow, double lr, double lambda, boolean verbose) {
        return new Opts(dim, useBias, negative, pow, lr, lambda, Float.NaN, false, verbose);
    }

    public int vectorSize() {
        return useBias ? dim + 1 : dim;
    }
}
