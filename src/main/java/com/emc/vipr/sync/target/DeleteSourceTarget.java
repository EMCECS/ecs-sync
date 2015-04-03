package com.emc.vipr.sync.target;

import com.emc.vipr.sync.filter.SyncFilter;
import com.emc.vipr.sync.model.object.SyncObject;
import com.emc.vipr.sync.source.SyncSource;
import com.emc.vipr.sync.util.OptionBuilder;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;

import java.util.Iterator;

/**
 * A simple target to delete the source object. Calls
 * {@link com.emc.vipr.sync.source.SyncSource#delete(SyncObject)}. Useful for logging/mapping
 * filters to record the deletion.
 */
public class DeleteSourceTarget extends SyncTarget {
    public static final String DELETE_TARGET = "delete";

    public static final String DEFAULT_TARGET_ID = "/dev/null";
    public static final String TARGET_ID_OPTION = "deleted-target-id";
    public static final String TARGET_ID_DESC = "The identifier to assign to deleted objects. Defaults to " + DEFAULT_TARGET_ID +
            ". When used with a logging/mapping filter, this value is useful for auditing";
    public static final String TARGET_ID_ARG_NAME = "identifier-value";

    private String targetId = DEFAULT_TARGET_ID;

    private SyncSource source;

    @Override
    public boolean canHandleTarget(String targetUri) {
        return targetUri.equals(DELETE_TARGET);
    }

    @Override
    public Options getCustomOptions() {
        Options options = new Options();
        options.addOption(new OptionBuilder().withLongOpt(TARGET_ID_OPTION).withDescription(TARGET_ID_DESC)
                .hasArg().withArgName(TARGET_ID_ARG_NAME).create());
        return options;
    }

    @Override
    protected void parseCustomOptions(CommandLine line) {
        if (line.hasOption(TARGET_ID_OPTION))
            targetId = line.getOptionValue(TARGET_ID_OPTION);
    }

    @Override
    public void configure(SyncSource source, Iterator<SyncFilter> filters, SyncTarget target) {
        this.source = source;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void filter(SyncObject obj) {
        source.delete(obj);
        obj.setTargetIdentifier(targetId);
    }

    @Override
    public SyncObject reverseFilter(SyncObject obj) {
        throw new UnsupportedOperationException("verification is not supported with delete");
    }

    @Override
    public String getName() {
        return "Delete Target";
    }

    @Override
    public String getDocumentation() {
        return "Simple target to delete objects from the source system. Calls source.delete(object). Useful for " +
                "logging/mapping filters to record the deletion";
    }

    public String getTargetId() {
        return targetId;
    }

    public void setTargetId(String targetId) {
        this.targetId = targetId;
    }
}
