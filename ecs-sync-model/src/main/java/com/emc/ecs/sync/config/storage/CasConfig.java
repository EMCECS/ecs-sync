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
package com.emc.ecs.sync.config.storage;

import com.emc.ecs.sync.config.AbstractConfig;
import com.emc.ecs.sync.config.annotation.*;

import javax.xml.bind.annotation.XmlRootElement;

import static com.emc.ecs.sync.config.storage.CasConfig.URI_PREFIX;

@XmlRootElement
@StorageConfig(uriPrefix = URI_PREFIX)
@Label("CAS")
@Documentation("The CAS plugin is triggered by the URI pattern:\n" +
        "cas:[hpp:]//host[:port][,host[:port]...]?name=<name>,secret=<secret>\n" +
        "or cas:[hpp:]//host[:port][,host[:port]...]?<pea_file>\n" +
        "Note that <name> should be of the format <subtenant_id>:<uid> " +
        "when connecting to an Atmos system. " +
        "This is passed to the CAS SDK as the connection string " +
        "(you can use primary=, secondary=, etc. in the server hints). " +
        "To facilitate CAS migrations, sync from a CasStorage source to " +
        "a CasStorage target. Note that by default, verification of a CasStorage " +
        "object will also verify all blobs.")
public class CasConfig extends AbstractConfig {
    public static final String URI_PREFIX = "cas:";
    public static final String DEFAULT_APPLICATION_NAME = "ECS-Sync";
    public static final String DEFAULT_APPLICATION_VERSION = CasConfig.class.getPackage().getImplementationVersion();
    public static final String DEFAULT_DELETE_REASON = "Deleted by ECS-Sync";

    private String connectionString;
    private String applicationName = DEFAULT_APPLICATION_NAME;
    private String applicationVersion = DEFAULT_APPLICATION_VERSION;
    private String deleteReason = DEFAULT_DELETE_REASON;

    @UriGenerator
    public String getUri() {
        return URI_PREFIX + connectionString;
    }

    @UriParser
    public void setUri(String uri) {
        assert uri.startsWith(URI_PREFIX) : "invalid uri " + uri;

        connectionString = uri.substring(URI_PREFIX.length());
        if (!connectionString.startsWith("hpp:")) connectionString = "hpp:" + connectionString;
    }

    @Option(locations = Option.Location.Form, description = "The connection string passed to the CAS SDK. Should be of the form hpp://<host>[:port][,<host>[:port]...]?name=<name>,secret=<secret>")
    public String getConnectionString() {
        return connectionString;
    }

    public void setConnectionString(String connectionString) {
        this.connectionString = connectionString;
    }

    @Option(description = "This is the application name given to the pool during initial connection.")
    public String getApplicationName() {
        return applicationName;
    }

    public void setApplicationName(String applicationName) {
        this.applicationName = applicationName;
    }

    @Option(description = "This is the application version given to the pool during initial connection.")
    public String getApplicationVersion() {
        return applicationVersion;
    }

    public void setApplicationVersion(String applicationVersion) {
        this.applicationVersion = applicationVersion;
    }

    @Option(valueHint = "audit-string", description = "When deleting source clips, this is the audit string.")
    public String getDeleteReason() {
        return deleteReason;
    }

    public void setDeleteReason(String deleteReason) {
        this.deleteReason = deleteReason;
    }

    public CasConfig withConnectionString(String connectionString) {
        setConnectionString(connectionString);
        return this;
    }
}
