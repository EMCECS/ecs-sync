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
package com.emc.ecs.sync.config.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface FilterConfig {
    /**
     * The --filter parameter value which will activate this filter
     * and add it to the filter chain.  The activation name should including only lowercase letters, numbers and dashes.
     * I.e. if you return "my-filter", then the CLI argument "--filter my-filter" will
     * activate that plugin and insert it into the chain at its corresponding place in the --filter options.  Multiple
     * filters are specified as "--filter filter1 --filter filter2 --filter filter3 ..."
     */
    String cliName();
}
