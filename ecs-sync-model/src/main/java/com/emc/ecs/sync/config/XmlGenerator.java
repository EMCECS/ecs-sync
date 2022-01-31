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

import org.springframework.beans.BeanWrapper;
import org.springframework.beans.PropertyAccessorFactory;
import org.springframework.core.convert.ConversionService;
import org.springframework.core.convert.support.DefaultConversionService;
import org.w3c.dom.Comment;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import java.io.StringWriter;
import java.util.*;

public final class XmlGenerator {
    private static final ConversionService conversionService = new DefaultConversionService();

    public static String generateXml(boolean addComments, boolean advancedOptions, String sourcePrefix, String targetPrefix, String... filterNames)
            throws InstantiationException, IllegalAccessException, ParserConfigurationException, TransformerException {
        // find config wrappers for given plugins
        ConfigWrapper<?> sourceWrapper = ConfigUtil.storageConfigWrapperFor(sourcePrefix);
        ConfigWrapper<?> targetWrapper = ConfigUtil.storageConfigWrapperFor(targetPrefix);
        List<ConfigWrapper<?>> filterWrappers = new ArrayList<>();
        if (filterNames != null) {
            for (String filterName : filterNames) {
                filterWrappers.add(ConfigUtil.filterConfigWrapperFor(filterName));
            }
        }

        DocumentBuilderFactory builderFactory = DocumentBuilderFactory.newInstance();
        builderFactory.setNamespaceAware(true);
        DocumentBuilder builder = builderFactory.newDocumentBuilder();

        Document document = builder.newDocument();
        Element rootElement = document.createElement("syncConfig");
        rootElement.setAttribute("xmlns", ConfigUtil.XML_NAMESPACE);
        document.appendChild(rootElement);

        // sync options
        rootElement.appendChild(createDefaultElement(document, ConfigUtil.wrapperFor(SyncOptions.class), "options", addComments, advancedOptions));

        // source
        Element sourceElement = document.createElement("source");
        rootElement.appendChild(sourceElement);
        if (addComments) {
            sourceElement.appendChild(document.createComment(" " + sourceWrapper.getDocumentation() + " "));
        }
        sourceElement.appendChild(createDefaultElement(document, sourceWrapper, null, addComments, advancedOptions));

        // filters
        if (filterNames != null && filterNames.length > 0) {
            Element filtersElement = document.createElement("filters");
            rootElement.appendChild(filtersElement);
            for (ConfigWrapper<?> filterWrapper : filterWrappers) {
                if (addComments) {
                    filtersElement.appendChild(document.createComment(" " + filterWrapper.getDocumentation() + " "));
                }
                filtersElement.appendChild(createDefaultElement(document, filterWrapper, null, addComments, advancedOptions));
            }
        }

        // target
        Element targetElement = document.createElement("target");
        rootElement.appendChild(targetElement);
        if (addComments) {
            targetElement.appendChild(document.createComment(" " + targetWrapper.getDocumentation() + " "));
        }
        targetElement.appendChild(createDefaultElement(document, targetWrapper, null, addComments, advancedOptions));

        // write XML
        StringWriter writer = new StringWriter();
        TransformerFactory factory = TransformerFactory.newInstance();
        factory.setAttribute("indent-number", 4);
        Transformer transformer = factory.newTransformer();
        transformer.setOutputProperty(OutputKeys.ENCODING, "UTF-8");
        transformer.setOutputProperty(OutputKeys.INDENT, "yes");
        transformer.setOutputProperty("{http://xml.apache.org/xslt}indent-amount", "4");
        transformer.transform(new DOMSource(document), new StreamResult(writer));

        return writer.toString();
    }

    private static <C> Element createDefaultElement(Document document, ConfigWrapper<C> configWrapper, String name, boolean addComments, boolean advancedOptions)
            throws IllegalAccessException, InstantiationException {

        // create main element
        if (name == null) name = initialLowerCase(configWrapper.getTargetClass().getSimpleName());
        Element mainElement = document.createElement(name);

        // create object instance for defaults
        C object = configWrapper.getTargetClass().newInstance();
        BeanWrapper beanWrapper = PropertyAccessorFactory.forBeanPropertyAccess(object);

        List<ConfigPropertyWrapper> propertyWrappers = new ArrayList<>();
        for (String property : configWrapper.propertyNames()) {
            propertyWrappers.add(configWrapper.getPropertyWrapper(property));
        }
        propertyWrappers.sort(Comparator.comparingInt(ConfigPropertyWrapper::getOrderIndex));

        for (ConfigPropertyWrapper propertyWrapper : propertyWrappers) {
            if (propertyWrapper.isAdvanced() && !advancedOptions) continue;

            Object defaultValue = beanWrapper.getPropertyValue(propertyWrapper.getName());

            // create XMl comment[s]
            if (addComments) {
                Comment comment = document.createComment(" " + propertyWrapper.getDescription() + " ");
                mainElement.appendChild(comment);
                String specString = propertyWrapper.getDescriptor().getPropertyType().getSimpleName();
                if (propertyWrapper.isRequired()) specString += " - Required";
                if (propertyWrapper.getDescriptor().getPropertyType().isArray())
                    specString += " - Repeat for multiple values";
                if (propertyWrapper.getValueList() != null && propertyWrapper.getValueList().length > 0)
                    specString += " - Values: " + Arrays.toString(propertyWrapper.getValueList());
                else if (propertyWrapper.getDescriptor().getPropertyType().isEnum())
                    specString += " - Values: " + Arrays.toString(propertyWrapper.getDescriptor().getPropertyType().getEnumConstants());
                if (defaultValue != null) specString += " - Default: " + defaultValue;
                comment = document.createComment(" " + specString + " ");
                mainElement.appendChild(comment);
            }

            // create XMl element
            Element propElement = document.createElement(propertyWrapper.getName());

            // set default value
            String defaultValueStr = propertyWrapper.getValueHint();
            if (defaultValue != null) defaultValueStr = conversionService.convert(defaultValue, String.class);
            if (defaultValueStr == null || defaultValueStr.length() == 0) defaultValueStr = propertyWrapper.getName();
            propElement.appendChild(document.createTextNode(defaultValueStr));

            // add to parent element
            mainElement.appendChild(propElement);
        }

        return mainElement;
    }

    private static String initialLowerCase(String value) {
        if (value.length() == 0) return value;
        String result = "" + Character.toLowerCase(value.charAt(0));
        if (value.length() > 1) return result + value.substring(1);
        return result;
    }

    private XmlGenerator() {
    }
}
