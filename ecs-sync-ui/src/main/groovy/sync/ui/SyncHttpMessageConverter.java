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
package sync.ui;

import com.emc.ecs.sync.config.ConfigUtil;
import com.emc.ecs.sync.config.ConfigWrapper;
import com.emc.ecs.sync.config.SyncConfig;
import com.emc.ecs.sync.rest.*;
import org.springframework.http.HttpHeaders;
import org.springframework.http.converter.HttpMessageConversionException;
import org.springframework.http.converter.xml.AbstractXmlHttpMessageConverter;
import sync.ui.migration.AbstractMigrationConfig;
import sync.ui.storage.AbstractStorage;

import javax.ws.rs.ext.ContextResolver;
import javax.ws.rs.ext.Provider;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.transform.Result;
import javax.xml.transform.Source;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@Provider
public class SyncHttpMessageConverter extends AbstractXmlHttpMessageConverter<Object> implements ContextResolver<JAXBContext> {
    public static final Class<?>[] jaxbClasses;
    public static final Class<?>[] allPluginClasses;

    static {
        // NOTE: Moxy has an issue when you add classes to the context that are already included
        // by way of property reference in another included class. I.e. JobInfo is already
        // referenced by JobList. If we add both JobList and JobInfo to the context, Moxy has a bug
        // where it sets the namespace of all classes to be the same (in our case,
        // http://www.emc.com/ecs/sync/model). This causes compatibility tests to fail because
        // previous versions used an empty namespace for all UI model classes
        List<Class<?>> classes = new ArrayList<>(Arrays.asList(
                JobList.class, JobControl.class, HostInfo.class, UiConfig.class,
                SyncResult.class, ScheduledSync.class, AbstractStorage.class,
                AbstractMigrationConfig.class));
        for (ConfigWrapper<?> wrapper : ConfigUtil.allStorageConfigWrappers()) {
            classes.add(wrapper.getTargetClass());
        }
        for (ConfigWrapper<?> wrapper : ConfigUtil.allFilterConfigWrappers()) {
            classes.add(wrapper.getTargetClass());
        }
        jaxbClasses = classes.toArray(new Class<?>[0]);

        // however, the http converter has to know about all plugin classes that may be sent over the wire
        classes.addAll(Arrays.asList(JobInfo.class, SyncConfig.class, SyncProgress.class));
        allPluginClasses = classes.toArray(new Class<?>[0]);
    }

    private JAXBContext jaxbContext;

    @Override
    protected Object readFromSource(Class<?> clazz, HttpHeaders headers, Source source) throws IOException {
        try {
            return getJaxbContext().createUnmarshaller().unmarshal(source);
        } catch (JAXBException e) {
            throw new HttpMessageConversionException("Could not create JAXB context", e);
        }
    }

    @Override
    protected void writeToResult(Object o, HttpHeaders headers, Result result) throws IOException {
        try {
            getJaxbContext().createMarshaller().marshal(o, result);
        } catch (JAXBException e) {
            throw new HttpMessageConversionException("Could not create JAXB context", e);
        }
    }

    @Override
    protected boolean supports(Class<?> clazz) {
        for (Class<?> pluginClass : allPluginClasses) {
            if (pluginClass.equals(clazz)) return true;
        }
        return false;
    }

    @Override
    public JAXBContext getContext(Class<?> type) {
        try {
            for (Class<?> knownClass : allPluginClasses) {
                if (knownClass == type) return getJaxbContext();
            }
            return null;
        } catch (JAXBException e) {
            throw new RuntimeException("could not initialize JAXB context", e);
        }
    }

    private synchronized JAXBContext getJaxbContext() throws JAXBException {
        if (jaxbContext == null) {
            jaxbContext = JAXBContext.newInstance(jaxbClasses);
        }
        return jaxbContext;
    }
}
