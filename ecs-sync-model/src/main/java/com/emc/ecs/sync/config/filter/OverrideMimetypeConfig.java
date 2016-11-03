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
package com.emc.ecs.sync.config.filter;

import com.emc.ecs.sync.config.AbstractConfig;
import com.emc.ecs.sync.config.annotation.Documentation;
import com.emc.ecs.sync.config.annotation.FilterConfig;
import com.emc.ecs.sync.config.annotation.Label;
import com.emc.ecs.sync.config.annotation.Option;

import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement
@FilterConfig(cliName = "override-mimetype")
@Label("Override Mimetype")
@Documentation("This plugin allows you to override the default mimetype " +
        "of objects getting transferred. It is useful for instances " +
        "where the mimetype of an object cannot be inferred from " +
        "its extension or is nonstandard (not in Java's " +
        "mime.types file). You can also use the force option to " +
        "override the mimetype of all objects")
public class OverrideMimetypeConfig extends AbstractConfig {
    private String overrideMimetype;
    private boolean forceMimetype;

    @Option(required = true, valueHint = "mimetype", description = "Specifies the mimetype to use when an object has no default mimetype")
    public String getOverrideMimetype() {
        return overrideMimetype;
    }

    public void setOverrideMimetype(String overrideMimetype) {
        this.overrideMimetype = overrideMimetype;
    }

    @Option(description = "If specified, the mimetype will be overwritten regardless of its prior value")
    public boolean isForceMimetype() {
        return forceMimetype;
    }

    public void setForceMimetype(boolean forceMimetype) {
        this.forceMimetype = forceMimetype;
    }
}
