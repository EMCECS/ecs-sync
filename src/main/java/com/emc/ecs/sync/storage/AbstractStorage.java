/*
 * Copyright 2013-2017 EMC Corporation. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 * http://www.apache.org/licenses/LICENSE-2.0.txt
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */
package com.emc.ecs.sync.storage;

import com.emc.ecs.sync.AbstractPlugin;
import com.emc.ecs.sync.config.RoleType;
import com.emc.ecs.sync.filter.SyncFilter;
import com.emc.ecs.sync.model.ObjectSummary;
import com.emc.ecs.sync.model.SyncObject;
import com.emc.ecs.sync.util.PerformanceWindow;
import org.apache.commons.csv.CSVRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;

public abstract class AbstractStorage<C> extends AbstractPlugin<C> implements SyncStorage<C> {
    private static final Logger log = LoggerFactory.getLogger(AbstractStorage.class);

    // 500ms measurement interval, 20-second window
    private PerformanceWindow readPerformanceCounter = new PerformanceWindow(500, 20);
    private PerformanceWindow writePerformanceCounter = new PerformanceWindow(500, 20);
    private RoleType role;

    /**
     * Try to create an appropriate ObjectSummary representing the specified object. Exceptions are allowed and it is
     * not necessary to throw or recast to ObjectNotFoundException (that will be discovered later)
     */
    protected abstract ObjectSummary createSummary(String identifier);

    @Override
    public void configure(SyncStorage source, Iterator<SyncFilter> filters, SyncStorage target) {
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
        CSVRecord record = getListFileCsvRecord(listLine);

        ObjectSummary summary = null;
        try {
            if (options.isEstimationEnabled()) summary = createSummary(record.get(0));
        } catch (Exception e) {
            log.debug("creating default summary for {} due to error: {}", listLine, e.getMessage());
        }
        if (summary == null) summary = new ObjectSummary(record.get(0), false, 0);

        summary.setListFileRow(listLine);
        return summary;
    }

    @Override
    public String createObject(SyncObject object) {
        String identifier = getIdentifier(object.getRelativePath(), object.getMetadata().isDirectory());
        updateObject(identifier, object);
        return identifier;
    }

    @Override
    public void close() {
        try (PerformanceWindow readWindow = readPerformanceCounter;
             PerformanceWindow writeWindow = writePerformanceCounter) {
            super.close();
        }
    }

    @Override
    protected void finalize() throws Throwable {
        try {
            close();
        } finally {
            super.finalize(); // make sure we call super.finalize() no matter what!
        }
    }

    @Override
    public void delete(String identifier) {
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

    public RoleType getRole() {
        return role;
    }
}
