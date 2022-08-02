/*
 * Copyright (c) 2016-2021 Dell Inc. or its subsidiaries. All Rights Reserved.
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
package com.emc.ecs.sync.config;

import com.emc.ecs.sync.config.annotation.Option;
import com.emc.ecs.sync.config.annotation.Role;

import java.beans.PropertyDescriptor;
import java.util.*;

public class ConfigPropertyWrapper {
    private PropertyDescriptor descriptor;
    private Option option;
    private RoleType role;
    private org.apache.commons.cli.Option cliOption;
    private Map<String, org.apache.commons.cli.Option> prefixOptions = new HashMap<>();
    private Set<Option.Location> locations;

    public ConfigPropertyWrapper(PropertyDescriptor descriptor) {
        if (!descriptor.getReadMethod().isAnnotationPresent(Option.class))
            throw new IllegalArgumentException(descriptor.getName() + " is not an @Option");
        this.descriptor = descriptor;
        this.option = descriptor.getReadMethod().getAnnotation(Option.class);
        if (descriptor.getReadMethod().isAnnotationPresent(Role.class))
            this.role = descriptor.getReadMethod().getAnnotation(Role.class).value();
        this.locations = new HashSet<>(Arrays.asList(this.option.locations()));
        this.cliOption = ConfigUtil.cliOptionFromAnnotation(descriptor, getAnnotation(), null);
    }

    public boolean isCliOption() {
        return this.locations.contains(Option.Location.CLI);
    }

    public PropertyDescriptor getDescriptor() {
        return descriptor;
    }

    public String getName() {
        return descriptor.getName();
    }

    public Option getAnnotation() {
        return option;
    }

    public boolean isRequired() {
        return option.required();
    }

    public String getLabel() {
        return (option.label().trim().isEmpty()) ? ConfigUtil.labelize(getName()) : option.label();
    }

    public String getCliName() {
        return cliOption.getLongOpt();
    }

    public boolean isCliInverted() {
        return option.cliInverted();
    }

    public String getDescription() {
        return option.description();
    }

    public String[] getValueList() {
        return option.valueList();
    }

    public String getValueHint() {
        return option.valueHint();
    }

    public Option.FormType getFormType() {
        return option.formType();
    }

    public int getOrderIndex() {
        return option.orderIndex();
    }

    public boolean isAdvanced() {
        return option.advanced();
    }

    public boolean isSensitive() {
        return option.sensitive();
    }

    public RoleType getRole() {
        return role;
    }

    public org.apache.commons.cli.Option getCliOption() {
        return cliOption;
    }

    public synchronized org.apache.commons.cli.Option getCliOption(String prefix) {
        if (prefix == null) return cliOption;
        org.apache.commons.cli.Option option = prefixOptions.get(prefix);
        if (option == null) {
            option = ConfigUtil.cliOptionFromAnnotation(getDescriptor(), getAnnotation(), prefix);
            prefixOptions.put(prefix, option);
        }
        return option;
    }
}
