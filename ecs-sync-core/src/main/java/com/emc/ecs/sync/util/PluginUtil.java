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
package com.emc.ecs.sync.util;

import com.emc.ecs.sync.SyncPlugin;
import com.emc.ecs.sync.config.SyncOptions;
import com.emc.ecs.sync.filter.InternalFilter;
import com.emc.ecs.sync.filter.SyncFilter;
import com.emc.ecs.sync.storage.SyncStorage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.context.annotation.ClassPathScanningCandidateComponentProvider;
import org.springframework.core.type.filter.AnnotationTypeFilter;
import org.springframework.core.type.filter.AssignableTypeFilter;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.ParameterizedType;
import java.util.*;

public final class PluginUtil {
    private static final Logger log = LoggerFactory.getLogger(PluginUtil.class);

    private static final ClassPathScanningCandidateComponentProvider pluginScanner;

    static {
        pluginScanner = new ClassPathScanningCandidateComponentProvider(false);
        pluginScanner.addIncludeFilter(new AssignableTypeFilter(SyncPlugin.class));
        pluginScanner.addExcludeFilter(new AnnotationTypeFilter(InternalFilter.class));
    }

    @SuppressWarnings("unchecked")
    public synchronized static <P extends SyncPlugin<C>, C> Class<C> configClassFor(Class<P> pluginClass) {
        try {
            ParameterizedType storageType = (ParameterizedType) pluginClass.getGenericSuperclass();
            return (Class<C>) storageType.getActualTypeArguments()[0];
        } catch (ClassCastException e) {
            log.warn("could not find config type for " + pluginClass.getSimpleName(), e);
            return null;
        }
    }

    public static <C> SyncStorage<C> newStorageFromConfig(C storageConfig, SyncOptions syncOptions) {
        return newPluginFromConfig(storageConfig, syncOptions);
    }

    public static List<SyncFilter<?>> newFiltersFromConfigList(List<?> filterConfigs, SyncOptions syncOptions) {
        List<SyncFilter<?>> filters = new ArrayList<>();
        for (Object filterConfig : filterConfigs) {
            filters.add(newFilterFromConfig(filterConfig, syncOptions));
        }
        return filters;
    }

    public static <C> SyncFilter<C> newFilterFromConfig(C filterConfig, SyncOptions syncOptions) {
        return newPluginFromConfig(filterConfig, syncOptions);
    }

    @SuppressWarnings("unchecked")
    private static <P extends SyncPlugin<C>, C> P newPluginFromConfig(C pluginConfig, SyncOptions syncOptions) {
        try {
            if (pluginCache.isEmpty()) loadPluginCache();
            Class<?> pClass = pluginCache.get(pluginConfig.getClass());

            if (pClass == null)
                throw new IllegalArgumentException("No plugin found that uses " + pluginConfig.getClass().getSimpleName());

            P plugin = (P) pClass.getDeclaredConstructor().newInstance();
            plugin.setConfig(pluginConfig);
            plugin.setOptions(syncOptions);
            return plugin;
        } catch (InstantiationException | IllegalAccessException | NoSuchMethodException | InvocationTargetException e) {
            throw new RuntimeException(e);
        }
    }

    private static final Map<Class<?>, Class<?>> pluginCache = new HashMap<>();

    private static synchronized void loadPluginCache() {
        if (pluginCache.isEmpty()) {
            for (Class<SyncPlugin<Object>> pClass : allPluginClasses()) {
                pluginCache.put(configClassFor(pClass), pClass);
            }
        }
    }

    private static <P extends SyncPlugin<C>, C> Iterable<Class<P>> allPluginClasses() {
        return () -> new PluginClassIterator<>(pluginScanner.findCandidateComponents("com/emc/ecs/sync").iterator());
    }

    private PluginUtil() {
    }

    private static class PluginClassIterator<P extends SyncPlugin<C>, C> extends ReadOnlyIterator<Class<P>> {
        private final Iterator<BeanDefinition> delegate;

        PluginClassIterator(Iterator<BeanDefinition> delegate) {
            this.delegate = delegate;
        }

        @SuppressWarnings("unchecked")
        @Override
        protected Class<P> getNextObject() {
            while (delegate.hasNext()) {
                BeanDefinition beanDef = delegate.next();
                try {
                    return (Class<P>) Class.forName(beanDef.getBeanClassName());
                } catch (ClassNotFoundException | NoClassDefFoundError e) {
                    log.warn("the {} plugin cannot be found: {}", beanDef.getBeanClassName(), e.toString());
                    log.debug("stacktrace:", e);
                } catch (UnsupportedClassVersionError e) {
                    String plugin = e.getMessage();
                    try {
                        plugin = plugin.substring(plugin.indexOf("Provider ") + 9).split(" ")[0];
                        plugin = plugin.substring(plugin.lastIndexOf(".") + 1);
                    } catch (Throwable t) {
                        // ignore
                    }
                    log.warn("the {} plugin is not supported in this version of java", plugin);
                    log.debug("stacktrace:", e);
                }
            }
            return null;
        }
    }
}
