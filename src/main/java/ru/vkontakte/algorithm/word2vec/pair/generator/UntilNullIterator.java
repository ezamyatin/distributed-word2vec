package ru.vkontakte.algorithm.word2vec.pair.generator;

import java.io.Serializable;
import java.util.Iterator;

/**
 * @author zamyatin-evg
 **/
public abstract class UntilNullIterator<T> implements Iterator<T>, Serializable {
    T next = generateOrNull();

    @Override
    public boolean hasNext() {
        return next != null;
    }

    @Override
    public T next() {
        T result = next;
        next = generateOrNull();
        return result;
    }

    public abstract T generateOrNull();
}
