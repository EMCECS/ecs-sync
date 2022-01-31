package com.emc.ecs.sync.config;

import javax.xml.bind.annotation.adapters.XmlAdapter;

public class StringListAdapter extends XmlAdapter<String, String[]> {
    @Override
    public String marshal(String[] v) {
        return String.join("\n", v);
    }

    @Override
    public String[] unmarshal(String v) {
        return v.split("\n");
    }
}
