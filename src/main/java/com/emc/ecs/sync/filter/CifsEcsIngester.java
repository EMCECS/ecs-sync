package com.emc.ecs.sync.filter;

import com.emc.ecs.sync.config.ConfigurationException;
import com.emc.ecs.sync.config.filter.CifsEcsConfig;
import com.emc.ecs.sync.model.ObjectContext;
import com.emc.ecs.sync.model.ObjectMetadata;
import com.emc.ecs.sync.model.SyncObject;
import com.emc.ecs.sync.storage.SyncStorage;
import com.emc.ecs.sync.storage.s3.AbstractS3Storage;
import org.apache.commons.csv.CSVRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.util.Iterator;

public class CifsEcsIngester extends AbstractFilter<CifsEcsConfig> {
    public static final Logger log = LoggerFactory.getLogger(CifsEcsIngester.class);

    public static final String MD_KEY_COMMON_ENCODING = "geodrive.common.encoding";
    public static final String MD_KEY_WINDOWS_ATTR = "geodrive.windows.attr";
    public static final String MD_KEY_WINDOWS_SECDESC = "geodrive.windows.secdesc";
    public static final String MD_KEY_WINDOWS_LONGNAME = "geodrive.windows.longname";

    @Override
    public void configure(SyncStorage source, Iterator<SyncFilter> filters, SyncStorage target) {
        super.configure(source, filters, target);

        if (!(target instanceof AbstractS3Storage))
            throw new ConfigurationException("CIFS ECS Ingester can only be used with an S3 target");
    }

    public void filter(ObjectContext objectContext) {
        ObjectMetadata metadata = objectContext.getObject().getMetadata();
        String relativePath = objectContext.getObject().getRelativePath();

        // EXPECTED CSV FORMAT:
        // {source-id},{relative-path},{cifs-ecs-encoding},{long-name},{attributes},{security-descriptor}
        CSVRecord fileRecord = null;
        if (objectContext.getSourceSummary().getListFileRow() != null)
            fileRecord = getListFileCsvRecord(objectContext.getSourceSummary().getListFileRow());

        if (fileRecord == null || fileRecord.size() < 6) {
            if (config.isFileMetadataRequired())
                throw new RuntimeException("File metadata not found for " + relativePath);
            log.warn("File metadata not found for {}", relativePath);

        } else { // we have metadata
            String encoding = fileRecord.get(2);
            String longName = fileRecord.get(3);
            String attributes = fileRecord.get(4);
            String securityDescriptor = fileRecord.get(5);

            log.debug("Setting CIFS ECS data for {}", objectContext.getObject().getRelativePath());
            if (encoding.length() > 0)
                metadata.setUserMetadataValue(MD_KEY_COMMON_ENCODING, encoding);
            if (attributes.length() > 0)
                metadata.setUserMetadataValue(MD_KEY_WINDOWS_ATTR, attributes);
            if (securityDescriptor.length() > 0)
                metadata.setUserMetadataValue(MD_KEY_WINDOWS_SECDESC, securityDescriptor);
            if (longName.length() > 0)
                metadata.setUserMetadataValue(MD_KEY_WINDOWS_LONGNAME, longName);
        }

        // directories in a CIFS-ECS bucket have a key of {dir-path}/_$folder$
        if (metadata.isDirectory()) {
            objectContext.getObject().setRelativePath(relativePath + "/_$folder$");
            metadata.setDirectory(false); // TODO: will this mess anything up?
            objectContext.getObject().setDataStream(new ByteArrayInputStream(new byte[0]));
        }

        // continue the filter chain
        getNext().filter(objectContext);
    }

    @Override
    public SyncObject reverseFilter(ObjectContext objectContext) {
        // TODO: if we ever verify metadata, we need to remove it here
        return getNext().reverseFilter(objectContext);
    }
}
