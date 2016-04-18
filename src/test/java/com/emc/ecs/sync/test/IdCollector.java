package com.emc.ecs.sync.test;

import com.emc.ecs.sync.filter.SyncFilter;
import com.emc.ecs.sync.model.object.SyncObject;
import com.emc.ecs.sync.source.SyncSource;
import com.emc.ecs.sync.target.SyncTarget;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class IdCollector extends SyncFilter {
    private Map<String, String> idMap = new HashMap<>();

    @Override
    public String getActivationName() {
        return "id-collector";
    }

    @Override
    protected void parseCustomOptions(CommandLine line) {
    }

    @Override
    public void configure(SyncSource source, Iterator<SyncFilter> filters, SyncTarget target) {
    }

    @Override
    public void filter(SyncObject obj) {
        idMap.put(obj.getSourceIdentifier(), null);
        getNext().filter(obj);
        idMap.put(obj.getSourceIdentifier(), obj.getTargetIdentifier());
    }

    @Override
    public SyncObject reverseFilter(SyncObject obj) {
        return getNext().reverseFilter(obj);
    }

    @Override
    public String getName() {
        return null;
    }

    @Override
    public String getDocumentation() {
        return null;
    }

    @Override
    public Options getCustomOptions() {
        return null;
    }

    public Map<String, String> getIdMap() {
        return new HashMap<>(idMap);
    }
}
