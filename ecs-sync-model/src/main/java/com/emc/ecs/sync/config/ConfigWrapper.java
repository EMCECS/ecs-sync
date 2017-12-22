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
package com.emc.ecs.sync.config;

import com.emc.ecs.sync.config.annotation.*;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeanWrapper;
import org.springframework.beans.PropertyAccessorFactory;

import java.beans.*;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;

public class ConfigWrapper<C> {
    private static final Logger log = LoggerFactory.getLogger(ConfigWrapper.class);

    public static String toString(Object value) {
        if (value == null) {
            return null;
        } else if (value instanceof int[]) {
            return Arrays.toString((int[]) value);
        } else if (value instanceof long[]) {
            return Arrays.toString((long[]) value);
        } else if (value instanceof float[]) {
            return Arrays.toString((float[]) value);
        } else if (value instanceof double[]) {
            return Arrays.toString((double[]) value);
        } else if (value instanceof short[]) {
            return Arrays.toString((short[]) value);
        } else if (value instanceof byte[]) {
            return Arrays.toString((byte[]) value);
        } else if (value instanceof char[]) {
            return Arrays.toString((char[]) value);
        } else if (value instanceof boolean[]) {
            return Arrays.toString((boolean[]) value);
        } else if (value instanceof Object[]) {
            return Arrays.toString((Object[]) value);
        } else {
            return value.toString();
        }
    }

    private Class<C> targetClass;
    private String uriPrefix;
    private String cliName;
    private String label;
    private String documentation;
    private RoleType role;
    private Map<String, ConfigPropertyWrapper> propertyMap = new LinkedHashMap<>();
    private Method uriParser;
    private Method uriGenerator;

    public ConfigWrapper(Class<C> targetClass) {
        try {
            this.targetClass = targetClass;
            if (targetClass.isAnnotationPresent(StorageConfig.class))
                this.uriPrefix = targetClass.getAnnotation(StorageConfig.class).uriPrefix();
            if (targetClass.isAnnotationPresent(FilterConfig.class))
                this.cliName = targetClass.getAnnotation(FilterConfig.class).cliName();
            if (targetClass.isAnnotationPresent(Label.class))
                this.label = targetClass.getAnnotation(Label.class).value();
            if (targetClass.isAnnotationPresent(Documentation.class))
                this.documentation = targetClass.getAnnotation(Documentation.class).value();
            if (targetClass.isAnnotationPresent(Role.class))
                this.role = targetClass.getAnnotation(Role.class).value();
            BeanInfo beanInfo = Introspector.getBeanInfo(targetClass);
            for (PropertyDescriptor descriptor : beanInfo.getPropertyDescriptors()) {
                if (descriptor.getReadMethod().isAnnotationPresent(Option.class)) {
                    propertyMap.put(descriptor.getName(), new ConfigPropertyWrapper(descriptor));
                }
            }
            for (MethodDescriptor descriptor : beanInfo.getMethodDescriptors()) {
                Method method = descriptor.getMethod();
                if (method.isAnnotationPresent(UriParser.class)) {
                    if (method.getReturnType().equals(Void.TYPE)
                            && method.getParameterTypes().length == 1 && method.getParameterTypes()[0].equals(String.class)) {
                        uriParser = method;
                    } else {
                        log.warn("illegal signature for @UriParser method {}.{}", targetClass.getSimpleName(), method.getName());
                    }
                } else if (method.isAnnotationPresent(UriGenerator.class)) {
                    if (method.getReturnType().equals(String.class) && method.getParameterTypes().length == 0) {
                        uriGenerator = method;
                    } else {
                        log.warn("illegal signature for @UriGenerator method {}.{}", targetClass.getSimpleName(), method.getName());
                    }
                }
            }
            if (propertyMap.isEmpty()) log.info("no @Option annotations found in {}", targetClass.getSimpleName());
        } catch (IntrospectionException e) {
            throw new RuntimeException(e);
        }
    }

    public Options getOptions() {
        return getOptions(null);
    }

    public Options getOptions(String prefix) {
        Options options = new Options();
        for (String name : propertyNames()) {
            ConfigPropertyWrapper propertyWrapper = getPropertyWrapper(name);
            if (propertyWrapper.isCliOption()) options.addOption(getPropertyWrapper(name).getCliOption(prefix));
        }
        return options;
    }

    public C parse(CommandLine commandLine) {
        return parse(commandLine, null);
    }

    public C parse(CommandLine commandLine, String prefix) {
        try {
            C object = getTargetClass().newInstance();
            BeanWrapper beanWrapper = PropertyAccessorFactory.forBeanPropertyAccess(object);

            for (String name : propertyNames()) {
                ConfigPropertyWrapper propertyWrapper = getPropertyWrapper(name);
                if (!propertyWrapper.isCliOption()) continue;

                org.apache.commons.cli.Option option = propertyWrapper.getCliOption(prefix);

                if (commandLine.hasOption(option.getLongOpt())) {

                    Object value = commandLine.getOptionValue(option.getLongOpt());
                    if (propertyWrapper.getDescriptor().getPropertyType().isArray())
                        value = commandLine.getOptionValues(option.getLongOpt());

                    if (Boolean.class == propertyWrapper.getDescriptor().getPropertyType()
                            || "boolean".equals(propertyWrapper.getDescriptor().getPropertyType().getName()))
                        value = Boolean.toString(!propertyWrapper.isCliInverted());

                    beanWrapper.setPropertyValue(name, value);
                }
            }

            return object;
        } catch (InstantiationException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    public void parseUri(C object, String uri) {
        if (uriParser != null) try {
            uriParser.invoke(object, uri);
        } catch (IllegalAccessException | InvocationTargetException e) {
            throw new RuntimeException(e);
        }
    }

    public String generateUri(C object) {
        try {
            if (uriGenerator != null) return (String) uriGenerator.invoke(object);
            else return uriPrefix;
        } catch (InvocationTargetException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    public String summarize(C object) {
        BeanWrapper beanWrapper = PropertyAccessorFactory.forBeanPropertyAccess(object);

        StringBuilder summary = new StringBuilder();
        if (getLabel() == null) summary.append(object.getClass().getSimpleName()).append("\n");
        else summary.append(getLabel()).append("\n");
        for (String name : propertyNames()) {
            summary.append(" - ").append(name).append(": ").append(toString(beanWrapper.getPropertyValue(name))).append("\n");
        }

        return summary.toString();
    }

    public Class<C> getTargetClass() {
        return targetClass;
    }

    public String getUriPrefix() {
        return uriPrefix;
    }

    public String getCliName() {
        return cliName;
    }

    public String getLabel() {
        return label;
    }

    public String getDocumentation() {
        return documentation;
    }

    public RoleType getRole() {
        return role;
    }

    public Iterable<String> propertyNames() {
        return new ArrayList<>(propertyMap.keySet());
    }

    public ConfigPropertyWrapper getPropertyWrapper(String name) {
        return propertyMap.get(name);
    }
}
