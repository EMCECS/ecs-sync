/*
 * Copyright (c) 2014-2021 Dell Inc. or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.emc.ecs.sync.util;

import java.util.*;

public class MultiValueMap<K, V> implements Map<K, List<V>>, Cloneable {
    private LinkedHashMap<K, List<V>> delegate = new LinkedHashMap<>();

    @Override
    public int size() {
        return delegate.size();
    }

    @Override
    public boolean isEmpty() {
        return delegate.isEmpty();
    }

    @Override
    public boolean containsKey(Object key) {
        return delegate.containsKey(key);
    }

    @Override
    public boolean containsValue(Object value) {
        return delegate.containsValue(value);
    }

    @Override
    public List<V> get(Object key) {
        return delegate.get(key);
    }

    @Override
    public List<V> put(K key, List<V> value) {
        return delegate.put(key, value);
    }

    public List<V> putSingle(K key, V value) {
        put(key, new ArrayList<V>());
        return add(key, value);
    }

    public List<V> add(K key, V value) {
        List<V> values = get(key);
        if (values == null) {
            values = new ArrayList<>();
            put(key, values);
        }
        values.add(value);
        return values;
    }

    @Override
    public List<V> remove(Object key) {
        return delegate.remove(key);
    }

    @Override
    public void putAll(Map<? extends K, ? extends List<V>> m) {
        delegate.putAll(m);
    }

    @Override
    public void clear() {
        delegate.clear();
    }

    @Override
    public Set<K> keySet() {
        return delegate.keySet();
    }

    @Override
    public Collection<List<V>> values() {
        return delegate.values();
    }

    @Override
    public Set<Entry<K, List<V>>> entrySet() {
        return delegate.entrySet();
    }

    @Override
    public boolean equals(Object o) {
        return delegate.equals(o);
    }

    @Override
    public int hashCode() {
        return delegate.hashCode();
    }

    @SuppressWarnings("unchecked")
    @Override
    public Object clone() throws CloneNotSupportedException {
        // necessary to support subclasses
        MultiValueMap<K, V> copy = (MultiValueMap<K, V>) super.clone();

        // make sure we actually clone our delegate
        copy.delegate = (LinkedHashMap<K, List<V>>) delegate.clone();

        // make sure value lists are cloned
        for (Entry<K, List<V>> entry : copy.entrySet()) {
            List<V> copiedValue = new ArrayList<>();
            copiedValue.addAll(entry.getValue());
            entry.setValue(copiedValue);
        }

        return copy;
    }

    @Override
    public String toString() {
        return "MultiValueMap{" + delegate + '}';
    }
}
