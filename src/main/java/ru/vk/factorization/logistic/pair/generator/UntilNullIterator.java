package ru.vk.factorization.logistic.pair.generator;

import javax.annotation.Nullable;
import java.io.Serializable;
import java.util.Iterator;
import java.util.NoSuchElementException;

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
        if (result == null) {
            throw new NoSuchElementException("next on empty iterator");
        }

        next = generateOrNull();
        return result;
    }

    @Nullable
    public abstract T generateOrNull();
}
