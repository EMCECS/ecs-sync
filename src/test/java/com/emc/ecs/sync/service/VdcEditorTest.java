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
package com.emc.ecs.sync.service;

import com.emc.ecs.sync.source.EcsS3Source;
import com.emc.rest.smart.ecs.Vdc;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.beans.BeanWrapperImpl;

import java.util.Arrays;

public class VdcEditorTest {
    @Test
    public void testToString() {
        Vdc vdc = new Vdc("foo", "bar", "baz");
        VdcEditor vdcEditor = new VdcEditor();
        vdcEditor.setValue(vdc);
        Assert.assertEquals("foo,bar,baz", vdcEditor.getAsText());

        vdc = new Vdc("uno", "dos", "tres").withName("alpha");
        vdcEditor.setValue(vdc);
        Assert.assertEquals("alpha(uno,dos,tres)", vdcEditor.getAsText());
    }

    @Test
    public void testFromString() {
        VdcEditor vdcEditor = new VdcEditor();
        vdcEditor.setAsText("1.1.1.1,50.50.50.50,255.255.255.255");
        Vdc vdc = (Vdc) vdcEditor.getValue();
        Assert.assertEquals("1.1.1.1", vdc.getName());
        Assert.assertEquals(3, vdc.getHosts().size());
        Assert.assertEquals("1.1.1.1", vdc.getHosts().get(0).getName());
        Assert.assertEquals("50.50.50.50", vdc.getHosts().get(1).getName());
        Assert.assertEquals("255.255.255.255", vdc.getHosts().get(2).getName());

        vdcEditor.setAsText("alpha(9.9.9.9,12.12.21.21,999.999.999.999)");
        vdc = (Vdc) vdcEditor.getValue();
        Assert.assertEquals("alpha", vdc.getName());
        Assert.assertEquals(3, vdc.getHosts().size());
        Assert.assertEquals("9.9.9.9", vdc.getHosts().get(0).getName());
        Assert.assertEquals("12.12.21.21", vdc.getHosts().get(1).getName());
        Assert.assertEquals("999.999.999.999", vdc.getHosts().get(2).getName());
    }

    @Test
    public void testBeanWrapperWithList() {
        EcsS3Source source = new EcsS3Source();
        BeanWrapperImpl wrapper = new BeanWrapperImpl(source);

        wrapper.registerCustomEditor(Vdc.class, new VdcEditor());

        wrapper.setPropertyValue("vdcs", Arrays.asList("foo(1.1.1.1,2.2.2.2)", "bar(3.3.3.3,4.4.4.4)"));

        Assert.assertEquals(2, source.getVdcs().size());
        Vdc foo = source.getVdcs().get(0);
        Assert.assertEquals("foo", foo.getName());
        Assert.assertEquals(2, foo.getHosts().size());
        Assert.assertEquals("1.1.1.1", foo.getHosts().get(0).getName());
        Assert.assertEquals("2.2.2.2", foo.getHosts().get(1).getName());
        Vdc bar = source.getVdcs().get(1);
        Assert.assertEquals("bar", bar.getName());
        Assert.assertEquals(2, bar.getHosts().size());
        Assert.assertEquals("3.3.3.3", bar.getHosts().get(0).getName());
        Assert.assertEquals("4.4.4.4", bar.getHosts().get(1).getName());
    }
}
