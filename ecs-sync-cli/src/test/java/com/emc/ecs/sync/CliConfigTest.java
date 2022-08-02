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
package com.emc.ecs.sync;

import com.emc.ecs.sync.config.ConfigUtil;
import com.emc.ecs.sync.config.ConfigWrapper;
import com.emc.ecs.sync.rest.LogLevel;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class CliConfigTest {
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

        Assertions.assertTrue(cliConfig.isHelp());
        Assertions.assertTrue(cliConfig.isVersion());
        Assertions.assertFalse(cliConfig.isRestEnabled());
        Assertions.assertTrue(cliConfig.isRestOnly());
        Assertions.assertEquals(db, cliConfig.getDbConnectString());
        Assertions.assertEquals(encPw, cliConfig.getDbEncPassword());
        Assertions.assertEquals(filters, cliConfig.getFilters());
        Assertions.assertEquals(rest, cliConfig.getRestEndpoint());
        Assertions.assertEquals(source, cliConfig.getSource());
        Assertions.assertEquals(target, cliConfig.getTarget());
        Assertions.assertEquals(xml, cliConfig.getXmlConfig());
        Assertions.assertEquals(log, cliConfig.getLogLevel());
    }
}
