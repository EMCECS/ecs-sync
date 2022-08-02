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

import com.emc.ecs.sync.config.annotation.FilterConfig;
import com.emc.ecs.sync.config.annotation.Option;
import com.emc.ecs.sync.config.annotation.StorageConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.context.annotation.ClassPathScanningCandidateComponentProvider;
import org.springframework.core.type.filter.AnnotationTypeFilter;

import java.beans.PropertyDescriptor;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;

public final class ConfigUtil {
    public static final String XML_NAMESPACE = "http://www.emc.com/ecs/sync/model";

    private static final Logger log = LoggerFactory.getLogger(ConfigUtil.class);

    private static final Map<Class<?>, ConfigWrapper<?>> wrapperCache = new HashMap<>();

    private static ClassPathScanningCandidateComponentProvider storageScanner;
    private static ClassPathScanningCandidateComponentProvider filterScanner;

    static {
        storageScanner = new ClassPathScanningCandidateComponentProvider(false);
        storageScanner.addIncludeFilter(new AnnotationTypeFilter(StorageConfig.class));
        filterScanner = new ClassPathScanningCandidateComponentProvider(false);
        filterScanner.addIncludeFilter(new AnnotationTypeFilter(FilterConfig.class));
    }

    @SuppressWarnings("unchecked")
    public static synchronized <C> ConfigWrapper<C> wrapperFor(Class<C> targetClass) {
        ConfigWrapper<C> configWrapper = (ConfigWrapper<C>) wrapperCache.get(targetClass);
        if (configWrapper == null) {
            configWrapper = new ConfigWrapper<>(targetClass);
            wrapperCache.put(targetClass, configWrapper);
        }
        return configWrapper;
    }

    @SuppressWarnings("unchecked")
    public static <C> void parseUri(C configObject, String uri) {
        wrapperFor((Class<C>) configObject.getClass()).parseUri(configObject, uri);
    }

    @SuppressWarnings("unchecked")
    public static <C> String generateUri(C configObject, boolean scrubbed) {
        return wrapperFor((Class<C>) configObject.getClass()).generateUri(configObject, scrubbed);
    }

    public static ConfigWrapper<?> storageConfigWrapperFor(String uri) {
        for (ConfigWrapper<?> wrapper : allStorageConfigWrappers()) {
            if (uri.startsWith(wrapper.getUriPrefix())) return wrapper;
        }
        throw new IllegalArgumentException("No storage config defined for URI " + uri);
    }

    public static ConfigWrapper<?> filterConfigWrapperFor(String cliName) {
        for (ConfigWrapper<?> wrapper : allFilterConfigWrappers()) {
            if (cliName.equals(wrapper.getCliName())) return wrapper;
        }
        throw new IllegalArgumentException("No filter config named " + cliName);
    }

    public static Iterable<ConfigWrapper<?>> allStorageConfigWrappers() {
        return new Iterable<ConfigWrapper<?>>() {
            @Override
            public Iterator<ConfigWrapper<?>> iterator() {
                return new WrapperIterator(storageScanner.findCandidateComponents("com.emc.ecs.sync").iterator());
            }
        };
    }

    public static Iterable<ConfigWrapper<?>> allFilterConfigWrappers() {
        return new Iterable<ConfigWrapper<?>>() {
            @Override
            public Iterator<ConfigWrapper<?>> iterator() {
                return new WrapperIterator(filterScanner.findCandidateComponents("com.emc.ecs.sync").iterator());
            }
        };
    }

    public static String hyphenate(String name) {
        StringBuilder hyphenated = new StringBuilder();
        for (char c : name.toCharArray()) {
            if (Character.isUpperCase(c) && hyphenated.length() > 0)
                hyphenated.append('-');
            hyphenated.append(Character.toLowerCase(c));
        }
        return hyphenated.toString();
    }

    public static String labelize(String name) {
        StringBuilder label = new StringBuilder();
        for (char c : name.toCharArray()) {
            if (Character.isUpperCase(c) && label.length() > 0)
                label.append(' ');
            label.append(label.length() == 0 ? Character.toUpperCase(c) : c);
        }
        return label.toString();
    }

    @SuppressWarnings("unchecked")
    public static <C> String summarize(C configObject) {
        return wrapperFor((Class<C>) configObject.getClass()).summarize(configObject);
    }

    public static void validate(Object configObject) {
        try {
            ConfigWrapper<?> wrapper = wrapperFor(configObject.getClass());
            for (String property : wrapper.propertyNames()) {
                ConfigPropertyWrapper propertyWrapper = wrapper.getPropertyWrapper(property);
                Object value = propertyWrapper.getDescriptor().getReadMethod().invoke(configObject);
                // check required
                if (propertyWrapper.isRequired() && value == null)
                    throw new ConfigurationException(wrapper.getTargetClass().getSimpleName() + "." + property + " is required");
            }
        } catch (InvocationTargetException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * convert an annotated getter into a commons-cli Option
     */
    public static org.apache.commons.cli.Option cliOptionFromAnnotation(PropertyDescriptor descriptor,
                                                                        Option _option,
                                                                        String prefix) {
        org.apache.commons.cli.Option option = new org.apache.commons.cli.Option(null, _option.description());

        // required
        if (_option.required()) option.setRequired(true);

        // long name
        String longName;
        if (_option.cliName().length() > 0) {
            longName = _option.cliName();
        } else {
            longName = hyphenate(descriptor.getName());
            if ((Boolean.class == descriptor.getPropertyType() || "boolean".equals(descriptor.getPropertyType().getName()))
                    && _option.cliInverted())
                longName = "no-" + longName;
        }
        if (prefix != null) longName = prefix + longName;
        option.setLongOpt(longName);

        // parameter[s]
        if (descriptor.getPropertyType().isArray()) {
            option.setArgs(org.apache.commons.cli.Option.UNLIMITED_VALUES);
        } else if (Boolean.class != descriptor.getPropertyType() && !"boolean".equals(descriptor.getPropertyType().getName())) {
            // non-booleans *must* have an argument
            option.setArgs(1);
        }
        if (option.hasArg()) {
            if (_option.valueHint().length() > 0) option.setArgName(_option.valueHint());
            else option.setArgName(option.getLongOpt());
        }

        return option;
    }

    public static String join(String[] parts) {
        if (parts == null || parts.length == 0) return null;
        StringBuilder joined = new StringBuilder();
        for (int i = 0; i < parts.length; i++) {
            joined.append(parts[i]);
            if (i < parts.length - 1) joined.append(",");
        }
        return joined.toString();
    }

    private ConfigUtil() {
    }

    private static class WrapperIterator implements Iterator<ConfigWrapper<?>> {
        private Iterator<BeanDefinition> definitionIterator;
        private ConfigWrapper<?> nextElement;

        public WrapperIterator(Iterator<BeanDefinition> definitionIterator) {
            this.definitionIterator = definitionIterator;
            findNext();
        }

        @Override
        public boolean hasNext() {
            return nextElement != null;
        }

        @Override
        public ConfigWrapper<?> next() {
            if (nextElement == null) throw new NoSuchElementException();
            ConfigWrapper<?> element = nextElement;
            findNext();
            return element;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException("this is a read-only iterator");
        }

        @SuppressWarnings("unchecked")
        private void findNext() {
            nextElement = null;
            while (definitionIterator.hasNext() && nextElement == null) {
                BeanDefinition beanDef = definitionIterator.next();
                try {
                    nextElement = wrapperFor(Class.forName(beanDef.getBeanClassName()));
                } catch (ClassNotFoundException e) {
                    log.warn("could not load plugin config " + beanDef.getBeanClassName(), e);
                } catch (UnsupportedClassVersionError e) {
                    log.warn("the plugin for " + beanDef.getBeanClassName() + " is not supported in this version of java", e);
                }
            }
        }
    }
}
