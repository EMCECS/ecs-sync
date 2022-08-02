/*
 * Copyright (c) 2020-2021 Dell Inc. or its subsidiaries. All Rights Reserved.
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
package com.emc.ecs.sync.storage.azure;

import com.emc.ecs.sync.config.ConfigurationException;
import com.emc.ecs.sync.config.storage.AzureBlobConfig;
import com.emc.ecs.sync.filter.SyncFilter;
import com.emc.ecs.sync.model.Checksum;
import com.emc.ecs.sync.model.ObjectMetadata;
import com.emc.ecs.sync.model.ObjectSummary;
import com.emc.ecs.sync.model.SyncObject;
import com.emc.ecs.sync.storage.AbstractStorage;
import com.emc.ecs.sync.storage.ObjectNotFoundException;
import com.emc.ecs.sync.storage.SyncStorage;
import com.emc.ecs.sync.util.Function;
import com.emc.ecs.sync.util.LazyValue;
import com.emc.ecs.sync.util.ReadOnlyIterator;
import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.net.URISyntaxException;
import java.security.InvalidKeyException;
import java.util.*;

public class AzureBlobStorage extends AbstractStorage<AzureBlobConfig> {
    private static final Logger log = LoggerFactory.getLogger(AzureBlobStorage.class);

    private static final String OPERATION_LIST_BLOBS = "AzureBlobListBlobs";
    private static final String OPERATION_GET_BLOB_REFERENCE = "AzureBlobGetBlobReference";
    private static final String OPERATION_READ_BLOB_STREAM = "AzureBlobReadBlobStream";

    public static final String PROP_BLOB_SNAPSHOTS = "azure.blobSnapshots";

    private CloudBlobClient blobClient;
    private CloudBlobContainer container;

    @Override
    public void configure(SyncStorage<?> source, Iterator<? extends SyncFilter<?>> filters, SyncStorage<?> target) {
        super.configure(source, filters, target);

        if (this == target) {
            throw new ConfigurationException("Azure blob storage is currently only supported as a source");
        }

        if (config.getContainerName() == null) {
            throw new ConfigurationException("need to set the container");
        }

        if (config.getConnectionString() == null) throw new ConfigurationException("Connection string is required");

        try {
            if (blobClient == null) {
                CloudStorageAccount storageAccount = CloudStorageAccount.parse(config.getConnectionString());
                blobClient = storageAccount.createCloudBlobClient();
            }

            container = blobClient.getContainerReference(config.getContainerName());
            log.info("get container: {}", container.getName());

        } catch (IllegalArgumentException | URISyntaxException | InvalidKeyException | StorageException e) {
            throw new ConfigurationException("error connecting to Azure blob storage account: " + e);
        }
    }

    @Override
    public String getRelativePath(String identifier, boolean directory) {
        return identifier;
    }

    @Override
    public String getIdentifier(String relativePath, boolean directory) {
        return relativePath;
    }

    @Override
    protected ObjectSummary createSummary(final String identifier) {
        BlobProperties blobProperties = getCloudBlobReference(identifier).getProperties();
        return new ObjectSummary(identifier, false, blobProperties.getLength());
    }

    @Override
    public Iterable<ObjectSummary> allObjects() {
        return () -> new PrefixIterator(config.getBlobPrefix());
    }

    private class PrefixIterator extends ReadOnlyIterator<ObjectSummary> {
        private final String prefix;
        private final List<ObjectSummary> objectSummaries = new ArrayList<>();
        private Iterator<ObjectSummary> itemIterator;

        PrefixIterator(String prefix) {
            this.prefix = prefix;
            getItemIterator();
        }

        private void getItemIterator() {
            Iterator<ListBlobItem> blobItemIterator = time(() -> container.listBlobs(prefix, true).iterator(), OPERATION_LIST_BLOBS);
            while (blobItemIterator.hasNext()) {
                ListBlobItem blobItem = blobItemIterator.next();
                if (blobItem instanceof CloudBlob) {
                    objectSummaries.add(new ObjectSummary(((CloudBlob) blobItem).getName(), false, ((CloudBlob) blobItem).getProperties().getLength()));
                }
            }

            log.info("total object Summaries:{}", objectSummaries.size());
            itemIterator = objectSummaries.iterator();
        }

        @Override
        protected ObjectSummary getNextObject() {
            if (itemIterator.hasNext()) {
                return itemIterator.next();
            } else {
                return null;
            }
        }
    }

    // TODO: implement directoryMode, using prefix+delimiter

    @Override
    public Iterable<ObjectSummary> children(ObjectSummary parent) {
        return Collections.emptyList();
    }

    @Override
    public SyncObject loadObject(String identifier) throws ObjectNotFoundException {
        return loadObject(identifier, config.isIncludeSnapShots());
    }


    private SyncObject loadObject(String identifier, boolean includeSnapShots) throws ObjectNotFoundException {
        if (identifier == null) {
            throw new ObjectNotFoundException(identifier);
        }

        if (includeSnapShots) {
            List<BlobSyncObject> snapshots = loadSnapshots(identifier);
            if (!snapshots.isEmpty()) {
                // use the latest version as source for return
                BlobSyncObject object = snapshots.get(snapshots.size() - 1);

                object.setProperty(PROP_BLOB_SNAPSHOTS, snapshots);

                return object;
            }
            throw new ObjectNotFoundException(identifier);
        } else {
            CloudBlob cloudBlob = getCloudBlobReference(identifier);
            ObjectMetadata metadata = syncMetaFromBlobProperties(cloudBlob.getProperties());
            SyncObject object = new SyncObject(this, getRelativePath(identifier, metadata.isDirectory()), metadata);

            LazyValue<InputStream> lazyStream = new LazyValue<InputStream>() {
                @Override
                public InputStream get() {
                    return getDataStream(cloudBlob);
                }
            };

            //TODO how to get ACL for BLOB

            object.setLazyStream(lazyStream);
            return object;
        }
    }

    private List<BlobSyncObject> loadSnapshots(final String key) {
        List<BlobSyncObject> snapshots = new ArrayList<>();

        for (ListBlobItem blobItem: container.listBlobs(key, true, EnumSet.of(BlobListingDetails.SNAPSHOTS), null, null)) {
            CloudBlob blob = (CloudBlob) blobItem;
            ObjectMetadata metadata = syncMetaFromBlobProperties(blob.getProperties());
            BlobSyncObject object = new BlobSyncObject(this, getRelativePath(key, metadata.isDirectory()), metadata);
            String snapshotId = "latest.version";
            if (blob.isSnapshot()) {
                snapshotId = blob.getSnapshotID();
            }

            object.setSnapshotId(snapshotId);
            LazyValue<InputStream> lazyStream = () -> getDataStream(blob);
            object.setLazyStream(lazyStream);
            snapshots.add(object);
        }
        log.debug("total blob {} objects(including snapshots) of blob : {}", key ,snapshots.size());
        return snapshots;
    }

    @Override
    public void updateObject(final String identifier, SyncObject object) {
        //since this plugin would not set as target, no need to override it now
    }

    private CloudBlob getCloudBlobReference(final String key) {
        return time(() -> {
            try {
                return container.getBlobReferenceFromServer(key);
            } catch (URISyntaxException | StorageException e) {
                throw new RuntimeException("Can not get blob reference from server for blob: " + key);
            }
        }, OPERATION_GET_BLOB_REFERENCE);
    }

    private ObjectMetadata syncMetaFromBlobProperties(BlobProperties properties) {
        ObjectMetadata metadata = new ObjectMetadata();

        metadata.setDirectory(false);
        metadata.setCacheControl(properties.getCacheControl());
        metadata.setContentLength(properties.getLength());
        metadata.setContentDisposition(properties.getContentDisposition());
        metadata.setContentEncoding(properties.getContentEncoding());
        if (properties.getContentMD5() != null) {
            // Content-MD5 should be in base64
            metadata.setChecksum(Checksum.fromBase64("MD5", properties.getContentMD5()));
        }
        metadata.setContentType(properties.getContentType());
        metadata.setModificationTime(properties.getLastModified());

        return metadata;
    }

    private InputStream getDataStream(final CloudBlob blob) {
        return time(new Function<InputStream>() {
            @Override
            public InputStream call() {
                try {
                    return blob.openInputStream();
                } catch (StorageException e) {
                    throw new RuntimeException("can not get data stream for blob: " + blob.getName());
                }
            }
        }, OPERATION_READ_BLOB_STREAM);
    }

    public CloudBlobClient getBlobClient() {
        return blobClient;
    }
}