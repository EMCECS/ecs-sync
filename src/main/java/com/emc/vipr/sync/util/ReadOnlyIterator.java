package com.emc.vipr.sync.util;

import java.util.Iterator;
import java.util.NoSuchElementException;

public abstract class ReadOnlyIterator<T> implements Iterator<T> {
    private T next;

    /**
     * Implement in subclasses to return the next object or null if there are no more objects.
     */
    protected abstract T getNextObject();

    @Override
    public final synchronized boolean hasNext() {
        if (next != null) return true;
        next = getNextObject();
        return next != null;
    }

    @Override
    public final synchronized T next() {
        if (hasNext()) {
            T theNext = next;
            next = null;
            return theNext;
        } else
            throw new NoSuchElementException("No more objects");
    }

    @Override
    public final void remove() {
        throw new UnsupportedOperationException("This is a read-only iterator");
    }
}
