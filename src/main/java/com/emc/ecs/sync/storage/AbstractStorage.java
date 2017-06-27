package com.emc.ecs.sync.storage;

import com.emc.ecs.sync.AbstractPlugin;
import com.emc.ecs.sync.config.RoleType;
import com.emc.ecs.sync.filter.SyncFilter;
import com.emc.ecs.sync.model.ObjectSummary;
import com.emc.ecs.sync.model.SyncObject;
import com.emc.ecs.sync.util.PerformanceWindow;
import org.apache.commons.csv.CSVRecord;

import java.util.Iterator;

public abstract class AbstractStorage<C> extends AbstractPlugin<C> implements SyncStorage<C> {
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

        ObjectSummary summary;
        try {
            summary = createSummary(record.get(0));
        } catch (Exception e) {
            summary = new ObjectSummary(record.get(0), false, 0);
        }

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
