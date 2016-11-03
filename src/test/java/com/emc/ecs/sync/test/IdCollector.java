package com.emc.ecs.sync.test;

import com.emc.ecs.sync.config.annotation.FilterConfig;
import com.emc.ecs.sync.filter.AbstractFilter;
import com.emc.ecs.sync.model.ObjectContext;
import com.emc.ecs.sync.model.SyncObject;

import java.util.HashMap;
import java.util.Map;

public class IdCollector extends AbstractFilter<IdCollector.IdCollectorConfig> {
    @Override
    public void filter(ObjectContext objectContext) {
        config.idMap.put(objectContext.getSourceSummary().getIdentifier(), null);
        getNext().filter(objectContext);
        config.idMap.put(objectContext.getSourceSummary().getIdentifier(), objectContext.getTargetId());
    }

    @Override
    public SyncObject reverseFilter(ObjectContext objectContext) {
        return getNext().reverseFilter(objectContext);
    }

    @FilterConfig(cliName = "id-collecting")
    public static class IdCollectorConfig {
        Map<String, String> idMap = new HashMap<>();

        public Map<String, String> getIdMap() {
            return new HashMap<>(idMap);
        }
    }
}
