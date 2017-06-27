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
package com.emc.ecs.sync.config;

import com.emc.ecs.sync.config.annotation.*;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConfigUtilTest {
    private static final Logger log = LoggerFactory.getLogger(ConfigUtilTest.class);

    @Test
    public void testFilterScanner() {
        long start = System.currentTimeMillis();

        ConfigWrapper<?> filterWrapper = ConfigUtil.filterConfigWrapperFor("foo");

        long time = System.currentTimeMillis() - start;
        log.warn("filter scanning took {}ms", time);

        Assert.assertEquals("Foo Filter", filterWrapper.getLabel());
        Assert.assertEquals("foo", filterWrapper.getCliName());

        start = System.currentTimeMillis();

        ConfigWrapper<?> storageWrapper = ConfigUtil.storageConfigWrapperFor("foo:");

        time = System.currentTimeMillis() - start;
        log.warn("storage scanning took {}ms", time);

        Assert.assertEquals("Foo Storage", storageWrapper.getLabel());
        Assert.assertEquals("foo:", storageWrapper.getUriPrefix());
    }

    @FilterConfig(cliName = "foo")
    @Label("Foo Filter")
    public static class FooFilterConfig {
    }

    @StorageConfig(uriPrefix = "foo:")
    @Label("Foo Storage")
    public static class FooStorageConfig {
    }

    @Test
    public void testHyphenate() {
        Assert.assertEquals("foo-bar-baz", ConfigUtil.hyphenate("fooBarBaz"));
        Assert.assertEquals("foo-bar-baz", ConfigUtil.hyphenate("FooBarBaz"));
    }

    @Test
    public void testLabelize() {
        Assert.assertEquals("Foo Bar Baz", ConfigUtil.labelize("fooBarBaz"));
        Assert.assertEquals("Foo Bar Baz", ConfigUtil.labelize("FooBarBaz"));
    }

    @Test
    public void testCliOptionGeneration() {
        Options options = ConfigUtil.wrapperFor(Foo.class).getOptions();

        assertOption(options.getOption("my-value"), "my-value", false, 1, "my-value");
        assertOption(options.getOption("my-num"), "my-num", false, 1, "my-num");
        assertOption(options.getOption("my-flag"), "my-flag", false, -1, null);
        assertOption(options.getOption("no-negative-flag"), "no-negative-flag", false, -1, null);
        assertOption(options.getOption("my-enum"), "my-enum", false, 1, "my-enum");
    }

    @Test
    public void testCliParse() throws Exception {
        String[] args = {
                "--my-value", "value",
                "--my-num", "7",
                "--no-negative-flag",
                "--my-enum", "TwoFoo",
                "--my-array", "one",
                "--my-array", "two",
                "--my-array", "three",
                "--my-flag"
        };

        ConfigWrapper<Foo> wrapper = ConfigUtil.wrapperFor(Foo.class);

        CommandLine commandLine = new DefaultParser().parse(wrapper.getOptions(), args);

        Foo foo = new Foo();
        foo.setMyValue("value");
        foo.setMyNum(7);
        foo.setMyFlag(true);
        foo.setNegativeFlag(false);
        foo.setMyEnum(FooType.TwoFoo);
        foo.setMyArray(new String[]{"one", "two", "three"});

        Foo foo2 = wrapper.parse(commandLine);

        Assert.assertEquals(foo.getMyValue(), foo2.getMyValue());
        Assert.assertEquals(foo.getMyNum(), foo2.getMyNum());
        Assert.assertEquals(foo.isMyFlag(), foo2.isMyFlag());
        Assert.assertEquals(foo.isNegativeFlag(), foo2.isNegativeFlag());
        Assert.assertEquals(foo.getMyEnum(), foo2.getMyEnum());
        Assert.assertArrayEquals(foo.getMyArray(), foo2.getMyArray());
    }

    @Test
    public void testSingleValueInArray() throws Exception {
        String[] args = {"--my-array", "foo"};

        Foo foo = new Foo();
        foo.setMyArray(new String[]{"foo"});

        ConfigWrapper<Foo> configWrapper = ConfigUtil.wrapperFor(Foo.class);
        Foo foo2 = configWrapper.parse(new DefaultParser().parse(configWrapper.getOptions(), args));

        Assert.assertArrayEquals(foo.getMyArray(), foo2.getMyArray());
    }

    @Test
    public void testUriHandling() throws Exception {
        String uri = "yo://mama";
        Foo foo = new Foo();

        ConfigUtil.parseUri(foo, uri);

        Assert.assertEquals(uri, foo.getPath());
        Assert.assertEquals(uri, ConfigUtil.generateUri(foo));
    }

    @Test
    public void testRole() throws Exception {
        ConfigWrapper<Foo> configWrapper = ConfigUtil.wrapperFor(Foo.class);

        Assert.assertEquals(RoleType.Source, configWrapper.getRole());
    }

    private void assertOption(org.apache.commons.cli.Option option, String longOpt, boolean required, int args, String argName) {
        Assert.assertNull(option.getOpt());
        Assert.assertEquals(longOpt, option.getLongOpt());
        Assert.assertEquals(required, option.isRequired());
        Assert.assertEquals(args, option.getArgs());
        Assert.assertEquals(argName, option.getArgName());
    }

    @Role(RoleType.Source)
    public static class Foo {
        private String path;
        private String myValue;
        private int myNum;
        private boolean myFlag;
        private boolean negativeFlag = true;
        private FooType myEnum;
        private String[] myArray;

        @UriGenerator
        public String generateUri() {
            return path;
        }

        @UriParser
        public void parseUri(String uri) {
            this.path = uri;
        }

        public String getPath() {
            return path;
        }

        public void setPath(String path) {
            this.path = path;
        }

        @Option
        public String getMyValue() {
            return myValue;
        }

        public void setMyValue(String myValue) {
            this.myValue = myValue;
        }

        @Option
        public int getMyNum() {
            return myNum;
        }

        public void setMyNum(int myNum) {
            this.myNum = myNum;
        }

        @Option
        public boolean isMyFlag() {
            return myFlag;
        }

        public void setMyFlag(boolean myFlag) {
            this.myFlag = myFlag;
        }

        @Option(cliInverted = true)
        public boolean isNegativeFlag() {
            return negativeFlag;
        }

        public void setNegativeFlag(boolean negativeFlag) {
            this.negativeFlag = negativeFlag;
        }

        @Option
        public FooType getMyEnum() {
            return myEnum;
        }

        public void setMyEnum(FooType myEnum) {
            this.myEnum = myEnum;
        }

        @Option
        public String[] getMyArray() {
            return myArray;
        }

        public void setMyArray(String[] myArray) {
            this.myArray = myArray;
        }
    }

    public enum FooType {
        OneFoo, TwoFoo
    }
}
