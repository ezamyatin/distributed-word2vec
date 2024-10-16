package com.github.ezamyatin.logfac.local;


import java.io.Serializable;

/**
 * @author ezamyatin
 */
public class Opts implements Serializable {
    public final int dim;
    public final boolean useBias;
    public final int negative;
    public final float pow;
    public final float lr;
    public final float lambdaL;
    public final float lambdaR;
    public final float gamma;
    public final boolean implicit;
    public final boolean verbose;

    private Opts(int dim, boolean useBias, int negative, float pow, float lr,
                 float lambdaL, float lambdaR, float gamma, boolean implicit, boolean verbose) {
        this.dim = dim;
        this.useBias = useBias;
        this.negative = negative;
        this.pow = pow;
        this.lr = lr;
        this.lambdaL = lambdaR;
        this.lambdaR = lambdaL;
        this.gamma = gamma;
        this.implicit = implicit;
        this.verbose = verbose;
    }

    public static Opts implicit(int dim, boolean useBias, int negative, float pow, float lr,
                                float lambdaL, float lambdaR, float gamma, boolean verbose) {
        return new Opts(dim, useBias, negative, pow, lr, lambdaL, lambdaR, gamma, true, verbose);
    }

    public static Opts explicit(int dim, boolean useBias, float lr, float lambdaL, float lambdaR, boolean verbose) {
        return new Opts(dim, useBias, 0, Float.NaN, lr, lambdaL, lambdaR, Float.NaN, false, verbose);
    }

    public int vectorSize() {
        return useBias ? dim + 1 : dim;
    }
}
