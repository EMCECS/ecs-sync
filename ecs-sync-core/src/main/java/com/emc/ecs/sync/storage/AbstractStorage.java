/*
 * Copyright (c) 2016-2022 Dell Inc. or its subsidiaries. All Rights Reserved.
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
package com.emc.ecs.sync.storage;

import com.emc.ecs.sync.AbstractPlugin;
import com.emc.ecs.sync.SkipObjectException;
import com.emc.ecs.sync.config.RoleType;
import com.emc.ecs.sync.filter.SyncFilter;
import com.emc.ecs.sync.model.ObjectContext;
import com.emc.ecs.sync.model.ObjectSummary;
import com.emc.ecs.sync.model.SyncObject;
import com.emc.ecs.sync.util.Function;
import com.emc.ecs.sync.util.OperationDetails;
import com.emc.ecs.sync.util.OperationListener;
import com.emc.ecs.sync.util.PerformanceWindow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.Iterator;
import java.util.concurrent.Callable;

public abstract class AbstractStorage<C> extends AbstractPlugin<C> implements SyncStorage<C> {
    private static final Logger log = LoggerFactory.getLogger(AbstractStorage.class);

    // 500ms measurement interval, 10-second window
    private final PerformanceWindow readPerformanceCounter = new PerformanceWindow(500, 20);
    private final PerformanceWindow writePerformanceCounter = new PerformanceWindow(500, 20);

    private RoleType role;

    /**
     * Try to create an appropriate ObjectSummary representing the specified object. Exceptions are allowed and it is
     * not necessary to throw or recast to ObjectNotFoundException (that will be discovered later)
     */
    protected abstract ObjectSummary createSummary(String identifier);

    @Override
    public void configure(SyncStorage<?> source, Iterator<? extends SyncFilter<?>> filters, SyncStorage<?> target) {
        super.configure(source, filters, target);

        if (this == source) role = RoleType.Source;
        else if (this == target) role = RoleType.Target;
    }

    /**
     * Default implementation uses a CSV parser to extract the first value, then sets the raw line of text on the summary
     * to make it available to other plugins. Note that any overriding implementation *must* catch Exception from
     * {@link #createSummary(String)} and return a new zero-sized {@link ObjectSummary} for the identifier
     */
    @Override
    public ObjectSummary parseListLine(String listLine) {
        String identifier = options.isSourceListRawValues()
                ? listLine // take the complete raw value from the list file
                : getListFileCsvRecord(listLine).get(0);

        ObjectSummary summary = null;
        try {
            if (options.isEstimationEnabled()) summary = createSummary(identifier);
        } catch (Exception e) {
            log.debug("creating default summary for {} due to error: {}", listLine, e.getMessage());
        }
        if (summary == null) summary = new ObjectSummary(identifier, false, 0);

        summary.setListFileRow(listLine);
        return summary;
    }

    @Override
    public String createObject(SyncObject object) {
        String identifier = getIdentifier(object.getRelativePath(), object.getMetadata().isDirectory());
        updateObject(identifier, object);
        return identifier;
    }

    /**
     * Default implementation just calls skipIfExists()
     */
    @Override
    public void beforeUpdate(ObjectContext objectContext, SyncObject targetObject) {
        skipIfExists(objectContext, targetObject);
    }

    /**
     * Skips the object if size is equal and target has a newer mtime/ctime
     *
     * @throws SkipObjectException if target has equal size and newer mtime/ctime
     */
    protected void skipIfExists(ObjectContext objectContext, SyncObject targetObject) {
        SyncObject sourceObject = objectContext.getObject();
        Date sourceMtime = sourceObject.getMetadata().getModificationTime();
        Date targetMtime = targetObject.getMetadata().getModificationTime();
        Date sourceCtime = sourceObject.getMetadata().getMetaChangeTime();
        if (sourceCtime == null) sourceCtime = sourceMtime;
        Date targetCtime = targetObject.getMetadata().getMetaChangeTime();
        if (targetCtime == null) targetCtime = targetMtime;

        // need to check mtime (data changed) and ctime (MD changed)
        boolean newer = sourceMtime == null || sourceMtime.after(targetMtime) || sourceCtime.after(targetCtime);

        boolean differentSize = sourceObject.getMetadata().getContentLength() != targetObject.getMetadata().getContentLength();

        // it is possible that a child is created before its parent directory (due to its
        // task/thread executing faster). since we have no way of detecting that, we must *always*
        // update directory metadata
        if (!sourceObject.getMetadata().isDirectory()
                && !options.isForceSync() && !newer && !differentSize && objectContext.getFailures() == 0) {
            // object already exists on the target and is the same size and newer than source;
            // assume it is the same and skip it (use verification to check MD5)
            throw new SkipObjectException(String.format("%s is the same size and newer on the target; skipping", sourceObject.getRelativePath()));
        }
    }

    @Override
    public void close() {
        readPerformanceCounter.close();
        writePerformanceCounter.close();
        super.close();
    }

    @Override
    public void delete(String identifier, SyncObject object) {
        throw new UnsupportedOperationException(String.format("Delete is not supported by the %s plugin", getClass().getSimpleName()));
    }

    @Override
    public long getReadRate() {
        return readPerformanceCounter.getWindowRate();
    }

    @Override
    public long getWriteRate() {
        return writePerformanceCounter.getWindowRate();
    }

    @Override
    public PerformanceWindow getReadWindow() {
        return readPerformanceCounter;
    }

    @Override
    public PerformanceWindow getWriteWindow() {
        return writePerformanceCounter;
    }

    /**
     * Note: this function will also call {@link #time(Callable, String)}, so plugins do not need to worry about both.
     */
    protected <T> T operationWrapper(Callable<T> function, String operationName, SyncObject syncObject, String identifier) throws Exception {
        // efficiency shortcut
        if (syncJob == null || syncJob.getOperationListeners().isEmpty()) return time(function, operationName);

        // even though time() does its own measurement, there is no clean way to share that, so must track our own here
        long startTime = System.currentTimeMillis();
        T result = null;
        RuntimeException exception = null;

        // execute operation and capture output
        try {
            result = time(function, operationName);
        } catch (RuntimeException e) {
            exception = e;
        }

        // send notification
        long duration = System.currentTimeMillis() - startTime;
        for (OperationListener listener : syncJob.getOperationListeners()) {
            listener.operationComplete(
                    new OperationDetails()
                            .withRole(getRole())
                            .withOperation(operationName)
                            .withSyncObject(syncObject)
                            .withIdentifier(identifier)
                            .withDurationMs(duration)
                            .withException(exception));
        }

        // bubble up results to calling code
        if (exception != null) throw exception;
        return result;
    }

    /**
     * Note: this function will also call {@link #time(Function, String)}, so plugins do not need to worry about both.
     */
    protected <T> T operationWrapper(Function<T> function, String operationName, SyncObject syncObject, String identifier) {
        try {
            return operationWrapper((Callable<T>) function, operationName, syncObject, identifier);
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException("caught non-runtime exception from Function - this should never happen", e);
        }
    }

    public RoleType getRole() {
        return role;
    }
}
