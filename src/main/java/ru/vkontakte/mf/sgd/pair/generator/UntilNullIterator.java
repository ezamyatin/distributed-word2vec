package ru.vkontakte.mf.sgd.pair.generator;

import javax.annotation.Nullable;
import java.io.Serializable;
import java.util.Iterator;

/**
 * @author zamyatin-evg
 **/
public abstract class UntilNullIterator<T> implements Iterator<T>, Serializable {
    private T next = null;

    @Override
    public boolean hasNext() {
        if (next == null) {
            next = generateOrNull();
        }
        return next != null;
    }

    @Override
    public T next() {
        T result = next == null ? generateOrNull() : next;
        next = generateOrNull();
        return result;
    }

    @Nullable
    public abstract T generateOrNull();
}
