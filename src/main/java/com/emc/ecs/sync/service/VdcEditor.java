/*
 * Copyright 2013-2016 EMC Corporation. All Rights Reserved.
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
package com.emc.ecs.sync.service;

import com.emc.ecs.sync.util.SyncUtil;
import com.emc.rest.smart.Host;
import com.emc.rest.smart.ecs.Vdc;

import java.beans.PropertyEditorSupport;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class VdcEditor extends PropertyEditorSupport {
    public static final String IP_PATTERN = "[0-9]{1,3}(?:[.][0-9]{1,3}){3}";
    public static final String VDC_PATTERN = "^(?:([-_a-zA-Z0-9]+)[(])?(" + IP_PATTERN + ")(," + IP_PATTERN + ")?(," + IP_PATTERN + ")?[)]?$";

    @Override
    public String getAsText() {
        Vdc vdc = (Vdc) getValue();
        if (vdc.getHosts().isEmpty()) return "";
        List<String> names = new ArrayList<>();
        for (Host host : vdc.getHosts()) {
            names.add(host.getName());
        }
        if (vdc.getName() == null || vdc.getName().equals(vdc.getHosts().get(0).getName())) {
            return SyncUtil.join(names, ",");
        } else {
            return String.format("%s(%s)", vdc.getName(), SyncUtil.join(names, ","));
        }
    }

    @Override
    public void setAsText(String text) throws IllegalArgumentException {
        Matcher matcher = Pattern.compile(VDC_PATTERN).matcher(text);
        if (!matcher.matches()) throw new IllegalArgumentException("invalid VDC format");

        String name = matcher.group(1);
        List<Host> hosts = new ArrayList<>();
        for (int i = 2; i <= matcher.groupCount() && matcher.group(i) != null; i++) {
            hosts.add(new Host(matcher.group(i).replaceFirst("^,", "")));
        }
        if (name == null) setValue(new Vdc(hosts));
        else setValue(new Vdc(name, hosts));
    }
}
