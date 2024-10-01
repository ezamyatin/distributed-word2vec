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

    public Opts(int dim, boolean useBias, int negative, double pow, double lr, double lambda, float gamma, boolean implicit) {
        this.dim = dim;
        this.useBias = useBias;
        this.negative = negative;
        this.pow = pow;
        this.lr = lr;
        this.lambda = lambda;
        this.gamma = gamma;
        this.implicit = implicit;
    }

    public int vectorSize() {
        return useBias ? dim + 1 : dim;
    }
}
