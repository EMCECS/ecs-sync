package com.emc.ecs.sync.service;

public class DbField {
    public static DbField create(String name, Type type, int dimension, boolean nullable) {
        return new DbField(name, type, dimension, nullable);
    }

    private final String name;
    private final Type type;
    private final int dimension;
    private final boolean nullable;

    private DbField(String name, Type type, int dimension, boolean nullable) {
        this.name = name;
        this.type = type;
        this.dimension = dimension;
        this.nullable = nullable;
    }

    public String name() {
        return name;
    }

    public Type type() {
        return type;
    }

    public int dimension() {
        return dimension;
    }

    public boolean nullable() {
        return nullable;
    }

    @Override
    public String toString() {
        return name;
    }

    enum Type {
        string, bool, intNumber, bigIntNumber, floatNumber, datetime
    }
}
