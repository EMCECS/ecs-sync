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
import com.microsoft.azure.storage.*;
import com.microsoft.azure.storage.blob.*;
import org.apache.commons.csv.CSVRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.Assert;

import java.io.InputStream;
import java.net.URISyntaxException;
import java.security.InvalidKeyException;
import java.util.*;


public class AzureBlobStorage extends AbstractStorage<AzureBlobConfig> {
    private static final Logger log = LoggerFactory.getLogger(AzureBlobStorage.class);

    private static final String OPERATION_LIST_BLOBS = "AzureBlobListBlobs";
    private static final String OPERATION_GET_BLOB_REFERENCE = "AzureBlobGetBlobReference";
    private static final String OPERATION_READ_BLOB_STREAM = "AzureBlobReadBlobStream";

    private CloudBlobClient blobClient;
    private CloudBlobContainer container;

    @Override
    public void configure(SyncStorage source, Iterator<SyncFilter> filters, SyncStorage target) {
        super.configure(source, filters, target);

        if (this == target) {
            throw new ConfigurationException("Azure blob storage is currently only supported as a source");
        }

        if (config.getContainerName() == null) {
            throw new ConfigurationException("need to set the container");
        }

        Assert.hasText(config.getConnectionString());

        if (options.isVerifyOnly()) {
            throw new ConfigurationException("can not set it to verify only when the source is Azure blob storage for current version");
        }

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
        return () -> new PrefixIterator(config.getBlobPerfix());
    }

    private class PrefixIterator extends ReadOnlyIterator<ObjectSummary> {
        private String prefix;
        private List<ObjectSummary> objectSummaries = new ArrayList<>();
        private Iterator<ObjectSummary> itemIterator;

        PrefixIterator(String prefix) {
            this.prefix = prefix;
            getItemIterator();
        }

        private void getItemIterator() {
            Iterator<ListBlobItem> blobItemIterator = time(() -> container.listBlobs(prefix).iterator(), OPERATION_LIST_BLOBS);
            while (blobItemIterator.hasNext()) {
                listAllBlobSummary(objectSummaries, blobItemIterator.next(), prefix);
            }

            log.info("total objectSummaies:{}", objectSummaries.size());
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

        private void listAllBlobSummary(List<ObjectSummary> objectSummaries, ListBlobItem blob, String perfix) {
            if (blob instanceof CloudBlobDirectory) {
                try {
                    for (ListBlobItem blobItem : ((CloudBlobDirectory) blob).listBlobs(perfix)) {
                        listAllBlobSummary(objectSummaries, blobItem, perfix);
                    }
                } catch (URISyntaxException | StorageException e) {
                    throw new RuntimeException("error for list all Azure blobs");
                }
            } else if (blob instanceof CloudBlob) {
                objectSummaries.add(new ObjectSummary(((CloudBlob) blob).getName(), false, ((CloudBlob) blob).getProperties().getLength()));
            } else {
                throw new RuntimeException("wrong blob type: " + blob.getUri());
            }
        }
    }

    //use PrefixIterator to get all objects
    private class PrefixWithSnapshotIterator extends ReadOnlyIterator<ObjectSummary> {
        private String prefix;
        private List<ObjectSummary> objectSummaries = new ArrayList<>();
        private Iterator<ObjectSummary> itemIterator;

        PrefixWithSnapshotIterator(String prefix) {
            this.prefix = prefix;
            getItemIterator();
        }

        private void getItemIterator() {
            EnumSet<BlobListingDetails> listingDetails = EnumSet.of(BlobListingDetails.SNAPSHOTS);
            BlobRequestOptions options = null;
            OperationContext opContext = null;
            Map<String, List<CloudBlob>> originalBlobMap = new HashMap<>();
            Iterator<ListBlobItem> blobItemIterator = time(() -> container.listBlobs(prefix, true, listingDetails, options, opContext).iterator(), OPERATION_LIST_BLOBS);
            while (blobItemIterator.hasNext()) {
                listAllBlobSummaryWithSnapshots(originalBlobMap, blobItemIterator.next(), prefix, listingDetails, options, opContext);
            }
            log.info("total blobs (including snapshots): {}", originalBlobMap.size());
            getObjectSummaryIterator(originalBlobMap);
        }

        @Override
        protected ObjectSummary getNextObject() {
            if (itemIterator.hasNext()) {
                return itemIterator.next();
            } else {
                return null;
            }
        }

        private void listAllBlobSummaryWithSnapshots(Map<String, List<CloudBlob>> originalBlobMap,
                                                     ListBlobItem blob,
                                                     String prefix,
                                                     EnumSet<BlobListingDetails> listingDetails,
                                                     BlobRequestOptions options,
                                                     OperationContext opContext) {
            if (blob instanceof CloudBlobDirectory) {
                try {
                    for (ListBlobItem blobItem : ((CloudBlobDirectory) blob).listBlobs(prefix, true, listingDetails, options, opContext)) {
                        listAllBlobSummaryWithSnapshots(originalBlobMap, blobItem, prefix, listingDetails, options, opContext);
                    }
                } catch (URISyntaxException | StorageException e) {
                    throw new RuntimeException("error for list all Azure blobs");
                }
            } else if (blob instanceof CloudBlob) {
                CloudBlob cloudBlob = (CloudBlob) blob;
                if (originalBlobMap.containsKey(cloudBlob.getName())) {
                    originalBlobMap.get(cloudBlob.getName()).add(cloudBlob);
                } else {
                    List<CloudBlob> cloudBlobList = new ArrayList<>();
                    cloudBlobList.add(cloudBlob);
                    originalBlobMap.put(cloudBlob.getName(), cloudBlobList);
                }
            } else {
                throw new RuntimeException("wrong blob type: " + blob.getUri());
            }
        }

        private void getObjectSummaryIterator(Map<String, List<CloudBlob>> originalBlobMap) {
            for (Map.Entry entry: originalBlobMap.entrySet()) {
                long size = 0;
                Map<String, CloudBlob> blobsWithSnapShotsMap = new TreeMap<>();
                for (CloudBlob blob: (CloudBlob[]) entry.getValue()) {
                    String key = "latest";
                    if (blob.isSnapshot()) {

                        key = blob.getSnapshotID();
                    }
                    blobsWithSnapShotsMap.put(key, blob);
                    size += blob.getProperties().getLength();
                }
                ObjectSummary objectSummary = new ObjectSummary(entry.getKey().toString(), false, size);
                objectSummary.setBlobsWithSnapShotsMap(blobsWithSnapShotsMap);
                objectSummaries.add(objectSummary);
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
                    return readDataStream(cloudBlob);
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
            LazyValue<InputStream> lazyStream = new LazyValue<InputStream>() {
                @Override
                public InputStream get() {
                    return readDataStream(blob);
                }
            };
            object.setLazyStream(lazyStream);
            object.getDataStream();
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
        metadata.setContentLength(Long.MAX_VALUE);
        metadata.setContentDisposition(properties.getContentDisposition());
        metadata.setContentEncoding(properties.getContentEncoding());
        if (properties.getContentMD5() != null) { metadata.setChecksum(new Checksum("MD5", properties.getContentMD5())); }
        metadata.setContentType(properties.getContentType());
        metadata.setModificationTime(properties.getLastModified());
        metadata.setBlobObjectLength(properties.getLength());

        return metadata;
    }

    private InputStream readDataStream(final CloudBlob blob) {
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
