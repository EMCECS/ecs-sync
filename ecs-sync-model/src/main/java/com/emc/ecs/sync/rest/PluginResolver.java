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
package com.emc.ecs.sync.rest;

import com.emc.ecs.sync.config.SyncConfig;
import com.emc.ecs.sync.config.annotation.FilterConfig;
import com.emc.ecs.sync.config.annotation.StorageConfig;
import com.emc.ecs.sync.rest.*;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.context.annotation.ClassPathScanningCandidateComponentProvider;
import org.springframework.core.type.filter.AnnotationTypeFilter;

import javax.ws.rs.ext.ContextResolver;
import javax.ws.rs.ext.Provider;
import javax.xml.bind.JAXBContext;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@Provider
public class PluginResolver implements ContextResolver<JAXBContext> {
    private JAXBContext context;

    public PluginResolver() throws Exception {
        ClassPathScanningCandidateComponentProvider pluginScanner = new ClassPathScanningCandidateComponentProvider(false);
        pluginScanner.addIncludeFilter(new AnnotationTypeFilter(StorageConfig.class));
        pluginScanner.addIncludeFilter(new AnnotationTypeFilter(FilterConfig.class));

        final List<Class> pluginClasses = new ArrayList<>();
        pluginClasses.addAll(Arrays.asList(SyncConfig.class, ErrorList.class, HostInfo.class, JobControl.class,
                JobList.class, SyncProgress.class));
        for (BeanDefinition beanDef : pluginScanner.findCandidateComponents("com.emc.ecs.sync")) {
            pluginClasses.add(Class.forName(beanDef.getBeanClassName()));
        }

        context = JAXBContext.newInstance(pluginClasses.toArray(new Class[pluginClasses.size()]));
    }

    @Override
    public JAXBContext getContext(Class<?> type) {
        return context;
    }
}
