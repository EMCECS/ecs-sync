/*
 * Copyright (c) 2016-2022 Dell Inc. or its subsidiaries. All Rights Reserved.
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
package com.emc.ecs.sync.rest;

import com.emc.ecs.sync.config.SyncConfig;
import com.emc.ecs.sync.config.annotation.FilterConfig;
import com.emc.ecs.sync.config.annotation.StorageConfig;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.context.annotation.ClassPathScanningCandidateComponentProvider;
import org.springframework.core.type.filter.AnnotationTypeFilter;

import javax.ws.rs.ext.ContextResolver;
import javax.ws.rs.ext.Provider;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@Provider
public class PluginResolver implements ContextResolver<JAXBContext> {
    private JAXBContext context;

    public PluginResolver() {
        try {
            ClassPathScanningCandidateComponentProvider pluginScanner = new ClassPathScanningCandidateComponentProvider(false);
            pluginScanner.addIncludeFilter(new AnnotationTypeFilter(StorageConfig.class));
            pluginScanner.addIncludeFilter(new AnnotationTypeFilter(FilterConfig.class));

            final List<Class> pluginClasses = new ArrayList<>();
            pluginClasses.addAll(Arrays.asList(HostInfo.class, JobControl.class, JobList.class));
            for (BeanDefinition beanDef : pluginScanner.findCandidateComponents("com.emc.ecs.sync")) {
                pluginClasses.add(Class.forName(beanDef.getBeanClassName()));
            }

            context = JAXBContext.newInstance(pluginClasses.toArray(new Class[0]));
        } catch (ClassNotFoundException | JAXBException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public JAXBContext getContext(Class<?> type) {
        return context;
    }
}
