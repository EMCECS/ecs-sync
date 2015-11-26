/*
 * Copyright 2013-2015 EMC Corporation. All Rights Reserved.
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
package com.emc.ecs.sync;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

public final class CommonOptions {
    public static final int DEFAULT_BUFFER_SIZE = 256 * 1024; // 256k

    public static final String METADATA_ONLY_OPTION = "metadata-only";
    public static final String METADATA_ONLY_DESC = "Only synchronize metadata (in supported plugins)";

    public static final String IGNORE_METADATA_OPTION = "ignore-metadata";
    public static final String IGNORE_METADATA_DESC = "Ignore all metadata when syncing. Objects written to the target will not have any metadata set by the sync operation.";

    public static final String INCLUDE_ACL_OPTION = "include-acl";
    public static final String INCLUDE_ACL_DESC = "Include ACL information when syncing objects (in supported plugins)";

    public static final String IGNORE_INVALID_ACLS_OPTION = "ignore-invalid-acls";
    public static final String IGNORE_INVALID_ACLS_DESC = "If including ACL information when migrating objects, ignore any invalid entries (i.e. permissions or identities that don't exist in the target system).";

    public static final String INCLUDE_RETENTION_EXPIRATION_OPTION = "include-retention-expiration";
    public static final String INCLUDE_RETENTION_EXPIRATION_DESC = "Instructs retention/expiration information when syncing objects (in supported plugins). The target plugin will *attempt* to replicate retention/expiration for each object. Works only on plugins that support retention/expiration. If the target is an Atmos cloud, the target policy must enable retention/expiration immediately for this to work.";

    public static final String FORCE_OPTION = "force";
    public static final String FORCE_DESC = "Instructs supported target plugins to overwrite any existing objects";

    public static final String IO_BUFFER_SIZE_OPTION = "io-buffer-size";
    public static final String IO_BUFFER_SIZE_DESC = "Sets the buffer size to use when streaming data from the source to the target (supported plugins only). Defaults to " + (DEFAULT_BUFFER_SIZE / 1024) + "K";
    public static final String IO_BUFFER_SIZE_ARG_NAME = "byte-size";

    public static final String MONITOR_PERFORMANCE_OPTION = "monitor-performance";
    public static final String MONITOR_PERFORMANCE_DESC = "Enables performance monitoring for reads and writes on any plugin that supports it. This information is available via the REST service during a sync";

    public static Options getOptions() {
        Options opts = new Options();
        opts.addOption(Option.builder().desc(METADATA_ONLY_DESC).longOpt(METADATA_ONLY_OPTION).build());
        opts.addOption(Option.builder().desc(IGNORE_METADATA_DESC).longOpt(IGNORE_METADATA_OPTION).build());
        opts.addOption(Option.builder().desc(INCLUDE_ACL_DESC).longOpt(INCLUDE_ACL_OPTION).build());
        opts.addOption(Option.builder().desc(IGNORE_INVALID_ACLS_DESC).longOpt(IGNORE_INVALID_ACLS_OPTION).build());
        opts.addOption(Option.builder().desc(INCLUDE_RETENTION_EXPIRATION_DESC)
                .longOpt(INCLUDE_RETENTION_EXPIRATION_OPTION).build());
        opts.addOption(Option.builder().desc(FORCE_DESC).longOpt(FORCE_OPTION).build());
        opts.addOption(Option.builder().longOpt(IO_BUFFER_SIZE_OPTION).desc(IO_BUFFER_SIZE_DESC)
                .hasArg().argName(IO_BUFFER_SIZE_ARG_NAME).build());
        opts.addOption(Option.builder().longOpt(MONITOR_PERFORMANCE_OPTION).desc(MONITOR_PERFORMANCE_DESC).build());
        return opts;
    }

    private CommonOptions() {
    }
}
