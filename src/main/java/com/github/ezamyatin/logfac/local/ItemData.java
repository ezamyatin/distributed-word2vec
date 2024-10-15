package com.github.ezamyatin.logfac.local;

import java.io.Serializable;
import java.util.List;

/**
 * @author ezamyatin
 */
public class ItemData implements Serializable {
    final public static boolean TYPE_LEFT = false;
    final public static boolean TYPE_RIGHT = true;

    public boolean type;
    public long id;
    public long cn;
    public float[] f;

    public ItemData(boolean type, long id, long cn, float[] f) {
        this.type = type;
        this.id = id;
        this.cn = cn;
        this.f = f;
    }
}
