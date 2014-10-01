/*
 * Copyright 2013 EMC Corporation. All Rights Reserved.
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
package com.emc.vipr.sync;

import com.emc.vipr.sync.filter.SyncFilter;
import com.emc.vipr.sync.source.SyncSource;
import com.emc.vipr.sync.target.SyncTarget;
import com.emc.vipr.sync.util.Timeable;
import com.emc.vipr.sync.util.TimingUtil;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;

import java.util.Iterator;
import java.util.concurrent.Callable;

/**
 * Basic SyncPlugin parent class.  All plugins should inherit from this class.
 * To have your plugin participate in command-line parsing, implement the
 * following methods:
 * <ul>
 * <li>getName()</li>
 * <li>getDocumentation()</li>
 * <li>getCustomOptions()</li>
 * <li>parseCustomOptions()</li>
 * </ul>
 * If you do not want your plugin to configure itself via the command line,
 * (e.g. if you are using Spring), you can simply leave those methods empty.
 *
 * @author cwikj
 */
public abstract class SyncPlugin {
    protected boolean metadataOnly = false;
    protected boolean ignoreMetadata = false;
    protected boolean includeAcl = false;
    protected boolean ignoreInvalidAcls = false;
    protected boolean includeRetentionExpiration = false;
    protected boolean force = false;
    protected int bufferSize = CommonOptions.DEFAULT_BUFFER_SIZE;

    /**
     * The name of this plugin.  This will be used when advertising the plugin
     * on the command-line help.
     *
     * @return the plugin's name
     */
    public abstract String getName();

    /**
     * A description of the plugin's operation.  This will be printed in the
     * command-line help under the plugin's name and just before its
     * command-line argument list.
     *
     * @return the plugin's help documentation
     */
    public abstract String getDocumentation();

    /**
     * This method returns the Apache commons CLI "Options" object.  This
     * exposes any options <em>unique to the implementation</em>.  Options that
     * may be shared across multiple plugins are contained in {@link com.emc.vipr.sync.CommonOptions}
     * and are already parsed/available in this class.
     * <p/>
     * This method is called from {@link #parseOptions(org.apache.commons.cli.CommandLine)}
     *
     * @return the Options object containing custom options for this implementation
     */
    public abstract Options getCustomOptions();

    /**
     * Called from the CLI to parse custom options on the command line.  This method should
     * extract any options the plugin needs to configure itself.  If an argument has an invalid
     * setting, you can throw an IllegalArgumentException here.  Note that common options are already
     * parsed in this class and available to subclasses.
     *
     * @param line The arguments passed from the command line.
     */
    protected abstract void parseCustomOptions(CommandLine line);

    /**
     * If this plugin is to be included in the process, this method will be
     * called just before processing starts.  The source, target and filter plugins
     * will be established at this point.  This gives each plugin a chance to
     * configure itself, inspect all of the other plugins in the chain and throw
     * ConfigurationException if any errors or incompatibilities are discovered.
     * For instance, if this plugin requires the <code>rootFile</code> option and
     * it is not present, it should throw an exception here.
     * Also, if this plugin only supports Atmos targets in object mode, it could
     * validate that the target is an AtmosTarget and that the
     * target's namespaceRoot is null.  Any exception thrown here will stop the sync
     * from running.
     */
    public abstract void configure(SyncSource source, Iterator<SyncFilter> filters, SyncTarget target);

    /**
     * Override to add any necessary cleanup logic after the entire sync is complete (i.e. close file handles, streams,
     * DB connections, etc.)
     */
    public void cleanup() {
    }

    public final void parseOptions(CommandLine line) {
        metadataOnly = line.hasOption(CommonOptions.METADATA_ONLY_OPTION);
        ignoreMetadata = line.hasOption(CommonOptions.IGNORE_METADATA_OPTION);
        includeAcl = line.hasOption(CommonOptions.INCLUDE_ACL_OPTION);
        ignoreInvalidAcls = line.hasOption(CommonOptions.IGNORE_INVALID_ACLS_OPTION);
        includeRetentionExpiration = line.hasOption(CommonOptions.INCLUDE_RETENTION_EXPIRATION_OPTION);
        force = line.hasOption(CommonOptions.FORCE_OPTION);
        if (line.hasOption(CommonOptions.IO_BUFFER_SIZE_OPTION)) {
            bufferSize = Integer.parseInt(line.getOptionValue(CommonOptions.IO_BUFFER_SIZE_OPTION));
        }

        parseCustomOptions(line);
    }

    protected <T> T time(Timeable<T> timeable, String name) {
        return TimingUtil.time(this, name, timeable);
    }

    protected <T> T time(Callable<T> timeable, String name) throws Exception {
        return TimingUtil.time(this, name, timeable);
    }

    protected void timeOperationStart(String name) {
        TimingUtil.startOperation(this, name);
    }

    protected void timeOperationComplete(String name) {
        TimingUtil.completeOperation(this, name);
    }

    protected void timeOperationFailed(String name) {
        TimingUtil.failOperation(this, name);
    }

    public boolean isMetadataOnly() {
        return metadataOnly;
    }

    public void setMetadataOnly(boolean metadataOnly) {
        this.metadataOnly = metadataOnly;
    }

    public boolean isIgnoreMetadata() {
        return ignoreMetadata;
    }

    public void setIgnoreMetadata(boolean ignoreMetadata) {
        this.ignoreMetadata = ignoreMetadata;
    }

    public boolean isIncludeAcl() {
        return includeAcl;
    }

    public void setIncludeAcl(boolean includeAcl) {
        this.includeAcl = includeAcl;
    }

    public boolean isIgnoreInvalidAcls() {
        return ignoreInvalidAcls;
    }

    public void setIgnoreInvalidAcls(boolean ignoreInvalidAcls) {
        this.ignoreInvalidAcls = ignoreInvalidAcls;
    }

    public boolean isIncludeRetentionExpiration() {
        return includeRetentionExpiration;
    }

    public void setIncludeRetentionExpiration(boolean includeRetentionExpiration) {
        this.includeRetentionExpiration = includeRetentionExpiration;
    }

    public boolean isForce() {
        return force;
    }

    public void setForce(boolean force) {
        this.force = force;
    }

    public int getBufferSize() {
        return bufferSize;
    }

    public void setBufferSize(int bufferSize) {
        this.bufferSize = bufferSize;
    }
}
