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

import com.emc.ecs.sync.config.SyncOptions;
import com.emc.ecs.sync.model.SyncObject;
import com.emc.ecs.sync.util.EnhancedThreadPoolExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingDeque;

public class Md5Verifier implements SyncVerifier {
    private static final Logger log = LoggerFactory.getLogger(Md5Verifier.class);

    private ExecutorService executor;

    public Md5Verifier(SyncOptions syncOptions) {
        executor = new EnhancedThreadPoolExecutor(syncOptions.getThreadCount() * 2, new LinkedBlockingDeque<Runnable>(), "verify-pool");
    }

    @Override
    public void verify(final SyncObject sourceObject, final SyncObject targetObject) {

        // this implementation only verifies data objects
        if (sourceObject.getMetadata().isDirectory()) {
            if (!targetObject.getMetadata().isDirectory())
                throw new RuntimeException("source is directory; target is not");
        } else {
            if (targetObject.getMetadata().isDirectory())
                throw new RuntimeException("source is data object; target is directory");
            targetObject.compareSyncObject(sourceObject);
            // thread the streams for efficiency (in case of verify-only)
            Future<String> futureSourceMd5 = executor.submit(new Callable<String>() {
                @Override
                public String call() throws Exception {
                    return sourceObject.getMd5Hex(true);
                }
            });
            Future<String> futureTargetMd5 = executor.submit(new Callable<String>() {
                @Override
                public String call() throws Exception {
                    return targetObject.getMd5Hex(true);
                }
            });

            try {
                String sourceMd5 = futureSourceMd5.get(), targetMd5 = futureTargetMd5.get();

                if (!sourceMd5.equals(targetMd5))
                    throw new RuntimeException(String.format("MD5 sum mismatch (%s != %s)", sourceMd5, targetMd5));
                else
                    log.debug("MD5 sum verified ({} == {})", sourceMd5, targetMd5);

            } catch (Exception e) {
                if (e instanceof RuntimeException) throw (RuntimeException) e;
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public void close() throws Exception {
        List<Runnable> tasks = executor.shutdownNow();
        if (!tasks.isEmpty()) log.warn(tasks.size() + " verification tasks still running when closed");
    }
}
