package com.emc.ecs.sync.model;

import com.emc.ecs.sync.config.SyncOptions;

import java.util.concurrent.atomic.AtomicInteger;

public class ObjectContext {
    private ObjectSummary sourceSummary;
    private String targetId;
    private SyncObject object;
    private ObjectStatus status;
    private AtomicInteger failures = new AtomicInteger();
    private SyncOptions options;

    public ObjectSummary getSourceSummary() {
        return sourceSummary;
    }

    public void setSourceSummary(ObjectSummary sourceSummary) {
        this.sourceSummary = sourceSummary;
    }

    public String getTargetId() {
        return targetId;
    }

    public void setTargetId(String targetId) {
        this.targetId = targetId;
    }

    public SyncObject getObject() {
        return object;
    }

    public void setObject(SyncObject object) {
        this.object = object;
    }

    public ObjectStatus getStatus() {
        return status;
    }

    public void setStatus(ObjectStatus status) {
        this.status = status;
    }

    public int getFailures() {
        return failures.intValue();
    }

    public void incFailures() {
        failures.incrementAndGet();
    }

    public SyncOptions getOptions() {
        return options;
    }

    public void setOptions(SyncOptions options) {
        this.options = options;
    }

    public ObjectContext withSourceSummary(ObjectSummary sourceSummary) {
        this.sourceSummary = sourceSummary;
        return this;
    }

    public ObjectContext withTargetId(String targetId) {
        this.targetId = targetId;
        return this;
    }

    public ObjectContext withObject(SyncObject object) {
        this.object = object;
        return this;
    }

    public ObjectContext withStatus(ObjectStatus status) {
        this.status = status;
        return this;
    }

    public ObjectContext withOptions(SyncOptions options) {
        this.options = options;
        return this;
    }
}
