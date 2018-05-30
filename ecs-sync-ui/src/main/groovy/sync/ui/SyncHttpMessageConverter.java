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
package sync.ui;

import com.emc.ecs.sync.config.ConfigUtil;
import com.emc.ecs.sync.config.ConfigWrapper;
import com.emc.ecs.sync.config.SyncConfig;
import com.emc.ecs.sync.rest.*;
import org.springframework.http.HttpHeaders;
import org.springframework.http.converter.HttpMessageConversionException;
import org.springframework.http.converter.xml.AbstractXmlHttpMessageConverter;

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
    public static final Class<?>[] pluginClasses;

    static {
        List<Class<?>> classes = new ArrayList<>(Arrays.asList(
                JobList.class, JobInfo.class, SyncConfig.class, SyncProgress.class,
                JobControl.class, HostInfo.class, UiConfig.class,
                SyncResult.class, ScheduledSync.class));
        for (ConfigWrapper<?> wrapper : ConfigUtil.allStorageConfigWrappers()) {
            classes.add(wrapper.getTargetClass());
        }
        for (ConfigWrapper<?> wrapper : ConfigUtil.allFilterConfigWrappers()) {
            classes.add(wrapper.getTargetClass());
        }
        pluginClasses = classes.toArray(new Class<?>[classes.size()]);
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
        for (Class<?> pluginClass : pluginClasses) {
            if (pluginClass.equals(clazz)) return true;
        }
        return false;
    }

    @Override
    public JAXBContext getContext(Class<?> type) {
        try {
            for (Class<?> knownClass : pluginClasses) {
                if (knownClass == type) return getJaxbContext();
            }
            return null;
        } catch (JAXBException e) {
            throw new RuntimeException("could not initialize JAXB context", e);
        }
    }

    private synchronized JAXBContext getJaxbContext() throws JAXBException {
        if (jaxbContext == null) {
            jaxbContext = JAXBContext.newInstance(pluginClasses);
        }
        return jaxbContext;
    }
}
