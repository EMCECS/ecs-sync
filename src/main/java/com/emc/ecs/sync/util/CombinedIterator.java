package com.emc.ecs.sync.util;

import java.util.Iterator;
import java.util.List;

public class CombinedIterator<T> extends ReadOnlyIterator<T> {
    private List<? extends Iterator<T>> iterators;
    private int currentIterator = 0;

    public CombinedIterator(List<? extends Iterator<T>> iterators) {
        this.iterators = iterators;
    }

    @Override
    protected T getNextObject() {
        while (currentIterator < iterators.size()) {
            if (iterators.get(currentIterator).hasNext()) return iterators.get(currentIterator).next();
            currentIterator++;
        }

        return null;
    }
}
