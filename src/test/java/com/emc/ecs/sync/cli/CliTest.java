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
package com.emc.ecs.sync.cli;

import com.emc.ecs.sync.config.ConfigUtil;
import com.emc.ecs.sync.config.ConfigWrapper;
import com.emc.ecs.sync.rest.LogLevel;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.junit.Assert;
import org.junit.Test;

public class CliTest {
    @Test
    public void testCliConfigParsing() throws Exception {
        String db = "foo:bar", encPw = "myEncPassword";
        String filters = "myFilters", rest = "myRestEndpoint";
        String source = "mySource", target = "myTarget", xml = "myXmlFile";
        LogLevel log = LogLevel.silent;
        String[] args = {
                "--help",
                "--version",
                "--no-rest-server",
                "--rest-only",
                "--db-connect-string", db,
                "--db-enc-password", encPw,
                "--filters", filters,
                "--rest-endpoint", rest,
                "--source", source,
                "--target", target,
                "--xml-config", xml,
                "--log-level", log.toString(),
        };

        ConfigWrapper<CliConfig> wrapper = ConfigUtil.wrapperFor(CliConfig.class);
        CommandLine commandLine = new DefaultParser().parse(wrapper.getOptions(), args);
        CliConfig cliConfig = wrapper.parse(commandLine);

        Assert.assertTrue(cliConfig.isHelp());
        Assert.assertTrue(cliConfig.isVersion());
        Assert.assertFalse(cliConfig.isRestEnabled());
        Assert.assertTrue(cliConfig.isRestOnly());
        Assert.assertEquals(db, cliConfig.getDbConnectString());
        Assert.assertEquals(encPw, cliConfig.getDbEncPassword());
        Assert.assertEquals(filters, cliConfig.getFilters());
        Assert.assertEquals(rest, cliConfig.getRestEndpoint());
        Assert.assertEquals(source, cliConfig.getSource());
        Assert.assertEquals(target, cliConfig.getTarget());
        Assert.assertEquals(xml, cliConfig.getXmlConfig());
        Assert.assertEquals(log, cliConfig.getLogLevel());
    }
}
