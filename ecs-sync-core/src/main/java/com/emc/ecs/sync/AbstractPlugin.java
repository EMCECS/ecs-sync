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

import com.emc.ecs.sync.config.SyncOptions;
import com.emc.ecs.sync.filter.SyncFilter;
import com.emc.ecs.sync.storage.SyncStorage;
import com.emc.ecs.sync.util.Function;
import com.emc.ecs.sync.util.TimingUtil;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;

import java.io.IOException;
import java.io.StringReader;
import java.util.Iterator;
import java.util.concurrent.Callable;

/**
 * Basic SyncPlugin parent class.  All plugins should inherit from this class
 * and implement a configuration class that encapsulates the information needed
 * to properly configure the plugin for a given sync job.
 * To have your plugin participate in command-line, XML, or UI configuration,
 * annotate the configuration class with the following annotations:
 * <ul>
 * <li>{@link javax.xml.bind.annotation.XmlRootElement}</li>
 * <li>{@link com.emc.ecs.sync.config.annotation.StorageConfig} or {@link com.emc.ecs.sync.config.annotation.FilterConfig}</li>
 * <li>{@link com.emc.ecs.sync.config.annotation.Label}</li>
 * <li>{@link com.emc.ecs.sync.config.annotation.Documentation}</li>
 * </ul>
 * Each property getter of the config class should also have an appropriate
 * {@link com.emc.ecs.sync.config.annotation.Option} annotation.
 *
 * @author cwikj
 */
public abstract class AbstractPlugin<C> implements SyncPlugin<C> {
    protected C config;
    protected SyncOptions options = new SyncOptions();
    /**
     * NOTE: syncJob may be null
     */
    protected EcsSync syncJob;

    @Override
    public void configure(SyncStorage<?> source, Iterator<? extends SyncFilter<?>> filters, SyncStorage<?> target) {
        assert config != null : "config is null";
        assert options != null : "options is null";
    }

    /**
     * Override to perform any necessary cleanup logic after the entire sync is complete (i.e. close file handles,
     * streams, DB connections, etc.) NOTE: be sure to call super if you override this!
     */
    @Override
    public void close() {
    }

    protected CSVRecord getListFileCsvRecord(String listFileLine) {
        try {
            return CSVFormat.EXCEL.parse(new StringReader(listFileLine)).iterator().next();
        } catch (IOException e) {
            throw new RuntimeException("error parsing list file CSV record: " + listFileLine, e);
        }
    }

    protected <T> T time(Function<T> function, String name) {
        return TimingUtil.time(options, getTimingPrefix() + name, function);
    }

    protected <T> T time(Callable<T> timeable, String name) throws Exception {
        return TimingUtil.time(options, getTimingPrefix() + name, timeable);
    }

    protected void timeOperationStart(String name) {
        TimingUtil.startOperation(options, getTimingPrefix() + name);
    }

    protected void timeOperationComplete(String name) {
        TimingUtil.completeOperation(options, getTimingPrefix() + name);
    }

    protected void timeOperationFailed(String name) {
        TimingUtil.failOperation(options, getTimingPrefix() + name);
    }

    protected String getTimingPrefix() {
        return getClass().getSimpleName() + "::";
    }

    @Override
    public C getConfig() {
        return config;
    }

    @Override
    public void setConfig(C config) {
        this.config = config;
    }

    @Override
    public SyncOptions getOptions() {
        return options;
    }

    @Override
    public void setOptions(SyncOptions options) {
        this.options = options;
    }

    @Override
    public EcsSync getSyncJob() {
        return syncJob;
    }

    @Override
    public void setSyncJob(EcsSync syncJob) {
        this.syncJob = syncJob;
    }

    public AbstractPlugin<C> withConfig(C config) {
        setConfig(config);
        return this;
    }

    public AbstractPlugin<C> withOptions(SyncOptions options) {
        setOptions(options);
        return this;
    }

    public AbstractPlugin<C> withSyncJob(EcsSync syncJob) {
        setSyncJob(syncJob);
        return this;
    }
}
