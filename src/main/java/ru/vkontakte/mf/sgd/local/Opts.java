package ru.vkontakte.mf.sgd.local;


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

    public Opts(int dim, boolean useBias, int negative, double pow, double lr, double lambda) {
        this.dim = dim;
        this.useBias = useBias;
        this.negative = negative;
        this.pow = pow;
        this.lr = lr;
        this.lambda = lambda;
    }

    public int vectorSize() {
        return useBias ? dim + 1 : dim;
    }
}
