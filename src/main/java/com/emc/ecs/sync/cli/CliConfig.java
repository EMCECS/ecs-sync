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

import com.emc.ecs.sync.config.annotation.Option;
import com.emc.ecs.sync.rest.LogLevel;
import com.emc.ecs.sync.rest.RestServer;

public class CliConfig {
    private boolean help;
    private boolean version;
    private boolean restEnabled = true;
    private boolean restOnly;
    private String restEndpoint;
    private String dbConnectString;
    private String dbEncPassword;
    private String xmlConfig;
    private LogLevel logLevel;
    private int perfReportSeconds;
    private String source;
    private String target;
    private String filters;

    @Option(description = "Displays this help content")
    public boolean isHelp() {
        return help;
    }

    public void setHelp(boolean help) {
        this.help = help;
    }

    @Option(description = "Displays package version")
    public boolean isVersion() {
        return version;
    }

    public void setVersion(boolean version) {
        this.version = version;
    }

    @Option(cliName = "no-rest-server", cliInverted = true, description = "Disables the REST server")
    public boolean isRestEnabled() {
        return restEnabled;
    }

    public void setRestEnabled(boolean restEnabled) {
        this.restEnabled = restEnabled;
    }

    @Option(description = "Enables REST-only control. This will start the REST server and remain alive until manually terminated. Excludes all other options except --rest-endpoint")
    public boolean isRestOnly() {
        return restOnly;
    }

    public void setRestOnly(boolean restOnly) {
        this.restOnly = restOnly;
    }

    @Option(description = "Specified the host and port to use for the REST endpoint. Optional; defaults to " + RestServer.DEFAULT_HOST_NAME + ":" + RestServer.DEFAULT_PORT)
    public String getRestEndpoint() {
        return restEndpoint;
    }

    public void setRestEndpoint(String restEndpoint) {
        this.restEndpoint = restEndpoint;
    }

    @Option(description = "Enables the MySQL database engine and specifies the JDBC connect string to connect to the database (i.e. \"jdbc:mysql://localhost:3306/ecs_sync?user=foo&password=bar\")")
    public String getDbConnectString() {
        return dbConnectString;
    }

    public void setDbConnectString(String dbConnectString) {
        this.dbConnectString = dbConnectString;
    }

    @Option(description = "Specifies the encrypted password for the MySQL database")
    public String getDbEncPassword() {
        return dbEncPassword;
    }

    public void setDbEncPassword(String dbEncPassword) {
        this.dbEncPassword = dbEncPassword;
    }

    @Option(description = "Specifies an XML configuration file. In this mode, the XML file contains all of the configuration for the sync job. In this mode, most other CLI arguments are ignored.")
    public String getXmlConfig() {
        return xmlConfig;
    }

    public void setXmlConfig(String xmlConfig) {
        this.xmlConfig = xmlConfig;
    }

    @Option(description = "Sets the verbosity of logging (silent|quiet|verbose|debug). Default is quiet")
    public LogLevel getLogLevel() {
        return logLevel;
    }

    public void setLogLevel(LogLevel logLevel) {
        this.logLevel = logLevel;
    }

    @Option(valueHint = "seconds", description = "Report upload and download rates for the source and target plugins every <x> seconds to INFO logging.  Default is off (0)")
    public int getPerfReportSeconds() {
        return perfReportSeconds;
    }

    public void setPerfReportSeconds(int perfReportSeconds) {
        this.perfReportSeconds = perfReportSeconds;
    }

    @Option(valueHint = "source-uri",
            description = "The URI for the source storage. Examples:\n" +
                    "atmos:http://uid:secret@host:port\n" +
                    " '- Uses Atmos as the source; could also be https.\n" +
                    "file:///tmp/atmos/\n" +
                    " '- Reads from a directory\n" +
                    "archive:///tmp/atmos/backup.tar.gz\n" +
                    " '- Reads from an archive file\n" +
                    "s3:http://key:secret@host:port\n" +
                    " '- Reads from an S3 bucket\n" +
                    "Other plugins may be available. See their documentation for URI formats")
    public String getSource() {
        return source;
    }

    public void setSource(String source) {
        this.source = source;
    }

    @Option(valueHint = "target-uri",
            description = "The URI for the target storage. Examples:\n" +
                    "atmos:http://uid:secret@host:port\n" +
                    " '- Uses Atmos as the target; could also be https.\n" +
                    "file:///tmp/atmos/\n" +
                    " '- Writes to a directory\n" +
                    "archive:///tmp/atmos/backup.tar.gz\n" +
                    " '- Writes to an archive file\n" +
                    "s3:http://key:secret@host:port\n" +
                    " '- Writes to an S3 bucket\n" +
                    "Other plugins may be available. See their documentation for URI formats")
    public String getTarget() {
        return target;
    }

    public void setTarget(String target) {
        this.target = target;
    }

    @Option(valueHint = "filter-names", description = "The comma-delimited list of filters to apply to objects as they are synced. " +
            "Note that filters are applied in the order specified (via CLI, XML or UI)" +
            "Specify the activation names of the filters [returned from Filter.getActivationName()]. Examples:\n" +
            "    id-logging\n" +
            "    gladinet-mapping,strip-acls\n" +
            "Each filter may have additional custom parameters you may specify separately")
    public String getFilters() {
        return filters;
    }

    public void setFilters(String filters) {
        this.filters = filters;
    }
}
