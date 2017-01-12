/*
 * Copyright 2013-2016 EMC Corporation. All Rights Reserved.
 *
 * Licensed under the Apache License, Stringersion 2.0 (the "License").
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
package com.emc.ecs.sync.config;

import javax.xml.bind.annotation.adapters.XmlAdapter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class MapAdapter extends XmlAdapter<MapAdapter.FlatMap, Map<String, String>> {
    @Override
    public Map<String, String> unmarshal(FlatMap v) throws Exception {
        Map<String, String> map = new TreeMap<>();
        for (FlatMapEntry entry : v.entry) {
            map.put(entry.key, entry.value);
        }
        return map;
    }

    @Override
    public FlatMap marshal(Map<String, String> v) throws Exception {
        FlatMap flatMap = new FlatMap();
        for (String key : v.keySet()) {
            flatMap.entry.add(new FlatMapEntry(key, v.get(key)));
        }
        if (flatMap.entry.isEmpty()) return null;
        return flatMap;
    }

    public static class FlatMap {
        public List<FlatMapEntry> entry = new ArrayList<>();
    }

    public static class FlatMapEntry {
        public String key;
        public String value;

        public FlatMapEntry() {
        }

        public FlatMapEntry(String key, String value) {
            this.key = key;
            this.value = value;
        }
    }
}
