package com.emc.vipr.sync.model;

public class SqlIdAnnotation implements ObjectAnnotation {
    private Object sqlId;

    public SqlIdAnnotation(Object sqlId) {
        this.sqlId = sqlId;
    }

    public Object getSqlId() {
        return sqlId;
    }
}
