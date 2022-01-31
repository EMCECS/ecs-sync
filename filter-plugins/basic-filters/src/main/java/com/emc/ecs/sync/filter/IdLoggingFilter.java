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
package com.emc.ecs.sync.filter;

import com.emc.ecs.sync.config.filter.IdLoggingConfig;
import com.emc.ecs.sync.model.ObjectContext;
import com.emc.ecs.sync.model.SyncObject;
import com.emc.ecs.sync.storage.SyncStorage;
import com.emc.ecs.sync.config.ConfigurationException;

import java.io.*;
import java.util.Iterator;

/**
 * Logs the Input IDs to Output IDs
 *
 * @author cwikj
 */
public class IdLoggingFilter extends AbstractFilter<IdLoggingConfig> {
    private PrintWriter out;

    @Override
    public void configure(SyncStorage<?> source, Iterator<? extends SyncFilter<?>> filters, SyncStorage<?> target) {
        super.configure(source, filters, target);

        assert config.getIdLogFile() != null : "idLogFile is null";

        try {
            out = new PrintWriter(new BufferedWriter(new FileWriter(new File(config.getIdLogFile()))));
        } catch (FileNotFoundException e) {
            throw new ConfigurationException("log file not found", e);
        } catch (IOException e) {
            throw new RuntimeException("could not write to log file", e);
        }
    }

    @Override
    public void filter(ObjectContext objectContext) {
        try {
            getNext().filter(objectContext);
            out.println(objectContext.getSourceSummary().getIdentifier() + ", " + objectContext.getTargetId());
        } catch (RuntimeException e) {
            // Log the error
            out.println(objectContext.getSourceSummary().getIdentifier() + ", FAILED: " + e.getMessage());
            throw e;
        }
    }

    @Override
    public SyncObject reverseFilter(ObjectContext objectContext) {
        return getNext().reverseFilter(objectContext);
    }

    @Override
    public void close() {
        try (PrintWriter p = out) {
            super.close();
        }
    }
}
