package sync.ui;

import com.emc.ecs.sync.config.ConfigUtil;
import com.emc.ecs.sync.config.ConfigWrapper;
import com.emc.ecs.sync.config.SyncConfig;
import com.emc.ecs.sync.rest.*;
import org.springframework.http.HttpHeaders;
import org.springframework.http.converter.HttpMessageConversionException;
import org.springframework.http.converter.xml.AbstractXmlHttpMessageConverter;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.transform.Result;
import javax.xml.transform.Source;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class SyncHttpMessageConverter extends AbstractXmlHttpMessageConverter<Object> {
    public static final Class<?>[] pluginClasses;

    static {
        List<Class<?>> classes = new ArrayList<>(Arrays.asList(
                JobList.class, JobInfo.class, SyncConfig.class, SyncProgress.class,
                JobControl.class, ErrorList.class, HostInfo.class, UiConfig.class,
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

    private synchronized JAXBContext getJaxbContext() throws JAXBException {
        if (jaxbContext == null) {
            jaxbContext = JAXBContext.newInstance(pluginClasses);
        }
        return jaxbContext;
    }
}
