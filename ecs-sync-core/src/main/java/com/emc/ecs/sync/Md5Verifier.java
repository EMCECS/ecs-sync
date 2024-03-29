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
    private SyncOptions options;

    public Md5Verifier(SyncOptions syncOptions) {
        executor = new EnhancedThreadPoolExecutor(syncOptions.getThreadCount() * 2, new LinkedBlockingDeque<Runnable>(), "verify-pool");
        this.options = syncOptions;
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

            // XXX: this method does not belong here - must find a different way to negotiate snapshots/versions
            targetObject.compareSyncObject(sourceObject);

            // thread the streams for efficiency (in case of verify-only)
            Future<String> futureSourceMd5 = executor.submit(new Callable<String>() {
                @Override
                public String call() throws Exception {
                    return getMd5HexForObject(sourceObject);
                }
            });
            Future<String> futureTargetMd5 = executor.submit(new Callable<String>() {
                @Override
                public String call() throws Exception {
                    return getMd5HexForObject(targetObject);
                }
            });

            try {
                String sourceMd5Hex = futureSourceMd5.get(), targetMd5Hex = futureTargetMd5.get();

                if (!sourceMd5Hex.equals(targetMd5Hex))
                    throw new RuntimeException(String.format("MD5 sum mismatch (%s != %s)", sourceMd5Hex, targetMd5Hex));
                else
                    log.debug("MD5 sum verified ({} == {})", sourceMd5Hex, targetMd5Hex);

            } catch (Exception e) {
                if (e instanceof RuntimeException) throw (RuntimeException) e;
                throw new RuntimeException(e);
            }
        }
    }

    protected String getMd5HexForObject(SyncObject object) {
        if (options.isUseMetadataChecksumForVerification()
                && object.getMetadata().getChecksum() != null
                && object.getMetadata().getChecksum().getAlgorithm().equals("MD5")) {
            return object.getMetadata().getChecksum().getHexValue();
        } else {
            return object.getMd5Hex(true);
        }
    }

    @Override
    public void close() throws Exception {
        List<Runnable> tasks = executor.shutdownNow();
        if (!tasks.isEmpty()) log.warn(tasks.size() + " verification tasks still running when closed");
    }
}
