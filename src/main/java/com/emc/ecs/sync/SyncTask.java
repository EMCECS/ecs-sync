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
package com.emc.ecs.sync;

import com.emc.ecs.sync.filter.SyncFilter;
import com.emc.ecs.sync.model.*;
import com.emc.ecs.sync.service.DbService;
import com.emc.ecs.sync.service.SyncRecord;
import com.emc.ecs.sync.storage.SyncStorage;
import com.emc.ecs.sync.util.SyncUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;

public class SyncTask implements Runnable {
    public static final String PROP_FAILURE_COUNT = "syncTask.failureCount";

    private static final Logger log = LoggerFactory.getLogger(SyncTask.class);

    private ObjectContext objectContext;
    private SyncStorage source;
    private SyncFilter filterChain;
    private SyncVerifier verifier;
    private DbService dbService;
    private RetryHandler retryHandler;
    private SyncControl syncControl;
    private SyncStats syncStats;

    public SyncTask(ObjectContext objectContext, SyncStorage source, SyncFilter filterChain,
                    SyncVerifier verifier, DbService dbService, RetryHandler retryHandler, SyncControl syncControl,
                    SyncStats syncStats) {
        this.objectContext = objectContext;
        this.source = source;
        this.filterChain = filterChain;
        this.verifier = verifier;
        this.dbService = dbService;
        this.retryHandler = retryHandler;
        this.syncControl = syncControl;
        this.syncStats = syncStats;
    }

    @Override
    public void run() {
        String sourceId = objectContext.getSourceSummary().getIdentifier();

        if (!syncControl.isRunning()) {
            log.debug("aborting sync task because terminate() was called: " + sourceId);
            return;
        }

        boolean processed = false, recordExists = false;
        SyncRecord record;
        try {
            dbService.lock(sourceId);
            record = dbService.getSyncRecord(objectContext);
            recordExists = record != null;

            // this should lazy-load all but metadata from the storage; this is so we see ObjectNotFoundException here
            objectContext.setObject(source.loadObject(sourceId));

            // make sure target can see if the object is being retried (necessary in corner cases)
            objectContext.getObject().setProperty(PROP_FAILURE_COUNT, objectContext.getFailures());

            ObjectMetadata metadata = objectContext.getObject().getMetadata();

            // truncate milliseconds (the DB only stores to the second)
            Date mtime = null;
            if (metadata.getModificationTime() != null)
                mtime = new Date(metadata.getModificationTime().getTime() / 1000 * 1000);

            if (record != null && record.getTargetId() != null) objectContext.setTargetId(record.getTargetId());

            if (!objectContext.getOptions().isVerifyOnly()) {
                if (record == null || !record.getStatus().isSuccess()
                        || (mtime != null && record.getMtime() != null && mtime.after(record.getMtime()))) {

                    log.debug("O--+ syncing {} {}", metadata.isDirectory() ? "directory" : "object", sourceId);

                    objectContext.setStatus(ObjectStatus.InTransfer);
                    recordExists = dbService.setStatus(objectContext, null, !recordExists);

                    try {
                        filterChain.filter(objectContext);
                    } catch (Throwable t) {
                        if (t instanceof NonRetriableException) throw t;
                        // make sure this reference to the object is closed before the retry re-opens it
                        if (objectContext.getObject() != null) objectContext.getObject().close();
                        retryHandler.submitForRetry(source, objectContext, t);
                        return;
                    }

                    if (metadata.isDirectory())
                        log.info("O--O finished syncing directory {}", sourceId);
                    else
                        log.info("O--O finished syncing object {} ({} bytes transferred)", sourceId, objectContext.getObject().getBytesRead());

                    objectContext.setStatus(ObjectStatus.Transferred);
                    dbService.setStatus(objectContext, null, false);
                    processed = true;
                } else {
                    log.info("O--* skipping {} because it is up-to-date in the target", sourceId);
                }
            }

            if (objectContext.getOptions().isVerify() || objectContext.getOptions().isVerifyOnly()) {
                if (record == null || record.getStatus() != ObjectStatus.Verified
                        || (mtime != null && record.getMtime() != null && mtime.after(record.getMtime()))) {

                    log.debug("O==? verifying {} {}", sourceId, metadata.isDirectory() ? "directory" : "object");

                    objectContext.setStatus(ObjectStatus.InVerification);
                    recordExists = dbService.setStatus(objectContext, null, !recordExists);

                    try {
                        SyncObject targetObject = filterChain.reverseFilter(objectContext);

                        try {
                            verifier.verify(objectContext.getObject(), targetObject);
                        } finally {
                            try {
                                // be sure to close all object resources
                                targetObject.close();
                            } catch (Throwable t) {
                                log.warn("could not close target object (" + objectContext.getTargetId() + ")", t);
                            }

                            // capture target MD5 in object context
                            // - if any exceptions are thrown here, the targetMD5 will just be null
                            try {
                                objectContext.setTargetMd5(targetObject.getMd5Hex(false));
                            } catch (Throwable t) {
                                log.info("error getting MD5 from target object (" + objectContext.getTargetId() + ")", t);
                            }
                        }

                    } catch (Throwable t) {
                        if (!objectContext.getOptions().isVerifyOnly()) { // if we just copied the data and verification failed, we should retry
                            // make sure this reference to the object is closed before the retry re-opens it
                            if (objectContext.getObject() != null) objectContext.getObject().close();
                            retryHandler.submitForRetry(source, objectContext, t);
                            return;
                        } else throw t;
                    }

                    log.info("O==O verification successful for {}", sourceId);
                    objectContext.setStatus(ObjectStatus.Verified);
                    dbService.setStatus(objectContext, null, false);
                    processed = true;
                } else {
                    log.info("O==* skipping {} because it has already been verified", sourceId);
                }
            }

            if (processed) {
                syncStats.incObjectsComplete();
                syncStats.incBytesComplete(objectContext.getObject().getBytesRead());
            } else {
                syncStats.incObjectsSkipped();
                syncStats.incBytesSkipped(objectContext.getSourceSummary().getSize());
            }

            try { // delete object if the source supports deletion (implements the delete() method)
                if (objectContext.getOptions().isDeleteSource()) {
                    source.delete(sourceId);
                    log.info("X--O deleted {} from source", sourceId);
                    dbService.setDeleted(objectContext, !recordExists);
                }
            } catch (Throwable t) {
                log.warn("!--O could not delete {} from source: {}", sourceId, t);
            }

        } catch (Throwable t) {
            try {
                objectContext.setStatus(ObjectStatus.Error);
                dbService.setStatus(objectContext, SyncUtil.summarize(t), !recordExists);
            } catch (Throwable t2) {
                log.warn("error setting DB status", t2);
            }

            log.warn("O--! object " + sourceId + " failed", SyncUtil.getCause(t));

            syncStats.incObjectsFailed();
            if (objectContext.getOptions().isRememberFailed()) syncStats.addFailedObject(sourceId);

        } finally {
            dbService.unlock(sourceId);
            try {
                // be sure to close all object resources
                if (objectContext.getObject() != null) objectContext.getObject().close();
            } catch (Throwable t) {
                log.warn("could not close object resources", t);
            }
        }
    }

    public ObjectContext getObjectContext() {
        return objectContext;
    }

    public SyncStorage getSource() {
        return source;
    }

    public SyncFilter getFilterChain() {
        return filterChain;
    }

    public SyncVerifier getVerifier() {
        return verifier;
    }

    public DbService getDbService() {
        return dbService;
    }

    public RetryHandler getRetryHandler() {
        return retryHandler;
    }

    public SyncControl getSyncControl() {
        return syncControl;
    }

    public SyncStats getSyncStats() {
        return syncStats;
    }
}
