/*
 * Copyright (c) 2018-2021 Dell Inc. or its subsidiaries. All Rights Reserved.
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
package com.emc.ecs.sync.filter;

import com.emc.ecs.sync.NonRetriableException;
import com.emc.ecs.sync.config.ConfigurationException;
import com.emc.ecs.sync.config.filter.CasSingleBlobExtractorConfig;
import com.emc.ecs.sync.model.ObjectContext;
import com.emc.ecs.sync.model.ObjectMetadata;
import com.emc.ecs.sync.model.ObjectSummary;
import com.emc.ecs.sync.model.SyncObject;
import com.emc.ecs.sync.storage.ObjectNotFoundException;
import com.emc.ecs.sync.storage.SyncStorage;
import com.emc.ecs.sync.storage.cas.CasStorage;
import com.emc.ecs.sync.storage.cas.ClipSyncObject;
import com.emc.ecs.sync.storage.cas.EnhancedTag;
import com.emc.ecs.sync.util.LazyValue;
import com.emc.ecs.sync.util.SyncUtil;
import com.filepool.fplibrary.FPLibraryException;
import org.apache.commons.csv.CSVRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Pattern;

public class CasSingleBlobExtractor extends AbstractFilter<CasSingleBlobExtractorConfig> {
    private static final Logger log = LoggerFactory.getLogger(CasSingleBlobExtractor.class);
    private static final String BAD_CHARS_REGEX = "[^A-Za-z0-9-]";
    private static final Pattern BAD_CHARS_PATTERN = Pattern.compile(BAD_CHARS_REGEX);

    @Override
    public void configure(SyncStorage<?> source, Iterator<? extends SyncFilter<?>> filters, SyncStorage<?> target) {
        super.configure(source, filters, target);

        if (source instanceof CasStorage) ((CasStorage) source).setDirectivesExpected(true);

        if ((options.getSourceListFile() == null || options.getSourceListFile().trim().length() == 0)
                && config.getPathSource() == CasSingleBlobExtractorConfig.PathSource.CSV)
            throw new ConfigurationException("Path source set to CSV, but no source file provided.");

        if (config.getPathSource() == CasSingleBlobExtractorConfig.PathSource.Attribute) {
            if (config.getPathAttribute() == null) {
                throw new ConfigurationException("path source set to Attribute, but no path attribute name set");
            }
        }
    }

    @Override
    public void filter(ObjectContext objectContext) {
        String identifier = null;
        try {
            final SyncObject origObject = objectContext.getObject();
            ClipSyncObject clipObject = (ClipSyncObject) origObject;
            ObjectSummary summary = objectContext.getSourceSummary();
            ObjectMetadata metadata = origObject.getMetadata();

            identifier = clipObject.getClip().getClipID();

            String targetPath = null;
            switch (config.getPathSource()) {
                case ClipId:
                    targetPath = summary.getIdentifier();
                    break;
                case CSV:
                    if (summary.getListFileRow() == null)
                        throw new RuntimeException("No list file data for " + summary.getIdentifier() + " (are you using the recursive option?)");
                    CSVRecord fileRecord = getListFileCsvRecord(summary.getListFileRow());
                    if (fileRecord.size() < 2)
                        throw new RuntimeException("No path info for " + summary.getIdentifier() + "in CSV file");
                    targetPath = fileRecord.get(1);
                    break;
            }

            EnhancedTag blobTag = null;
            try {
                List<String> preserveBadAttributes = new ArrayList<>();
                for (EnhancedTag tag : clipObject.getTags()) {
                    String tagName = tag.getTag().getTagName();
                    if (tag.isBlobAttached()) {
                        if (blobTag == null) {
                            blobTag = tag;
                        } else {
                            throw new NonRetriableException(String.format("more than one blob found in clip %s", identifier));
                        }
                    }

                    // pull attributes off tag for UMD
                    String[] attributes = tag.getTag().getAttributes();
                    for (int i = 0; i < attributes.length; i = i + 2) {
                        try {
                            int dataIndex = i + 1;
                            String key = attributes[i];
                            String val = attributes[dataIndex];
                            if (config.getPathAttribute() != null &&
                                    config.getPathSource() == CasSingleBlobExtractorConfig.PathSource.Attribute &&
                                    key.equals(config.getPathAttribute())) {
                                targetPath = val;
                            }
                            if (BAD_CHARS_PATTERN.matcher(key).find()) {
                                switch (config.getAttributeNameBehavior()) {
                                    case FailTheClip:
                                        throw new NonRetriableException(String.format("found non ACSII characters in attribute %s " +
                                                "on tag %s, per configuration failing clip %s migration", key, tagName, identifier));
                                    case SkipBadName:
                                        log.warn(String.format("found non ACSII characters in attribute %s " +
                                                "on tag %s, per configuration skipping this attribute", key, tagName));
                                        continue; // skip adding this attribute
                                    case ReplaceBadCharacters:
                                        String originalKey = key;
                                        preserveBadAttributes.add(key);
                                        key = key.replaceAll(BAD_CHARS_REGEX, "-");
                                        log.warn(String.format("found non ACSII characters in attribute %s on tag %s, " +
                                                "per configuration replacing bad characters with '-' creating new key name %s", originalKey, tagName, key));
                                        break;
                                }
                            }
                            metadata.setUserMetadataValue(key, val);
                        } catch (IndexOutOfBoundsException e) {
                            throw new RuntimeException(String.format("error parsing attributes on tag %s", tagName), e);
                        }
                    }
                }
                // preserve converted attribute names
                if (!preserveBadAttributes.isEmpty())
                    metadata.setUserMetadataValue("x-emc-invalid-meta-names", SyncUtil.join(preserveBadAttributes, ","));

            } catch (FPLibraryException e) {
                throw new RuntimeException(CasStorage.summarizeError(e), e);
            }

            if (blobTag == null && !config.isMissingBlobsAreEmptyFiles())
                throw new NonRetriableException(String.format("blob not found for clip {%s}", identifier));

            if (config.getPathAttribute() != null && targetPath == null)
                throw new NonRetriableException(String.format("path attribute %s not found in clip %s", config.getPathAttribute(), clipObject.getClip().getClipID()));

            metadata.setContentLength(blobTag == null ? 0L : blobTag.getTag().getBlobSize());
            metadata.setModificationTime(new Date(clipObject.getClip().getCreationDate()));

            SyncObject extractedObject = new SyncObject(origObject.getSource(), targetPath, metadata,
                    origObject.getDataStream(), origObject.getAcl()) {
                // make sure we close the original object
                @Override
                public void close() throws Exception {
                    origObject.close();
                    super.close();
                }
            };
            objectContext.setObject(extractedObject);

            final EnhancedTag tag = blobTag;
            if (tag != null) {
                extractedObject.setDataStream(null);
                extractedObject.setLazyStream(new LazyValue<InputStream>() {
                    @Override
                    public InputStream get() {
                        try {
                            return tag.getBlobInputStream();
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    }
                });
            } else {
                extractedObject.setDataStream(new ByteArrayInputStream(new byte[0]));
            }


            getNext().filter(objectContext);

        } catch (Throwable t) {
            if (t instanceof FPLibraryException) {
                FPLibraryException e = (FPLibraryException) t;
                if (e.getErrorCode() == -10021) throw new ObjectNotFoundException(identifier);
                throw new RuntimeException(CasStorage.summarizeError(e), e);
            } else if (t instanceof RuntimeException) {
                throw (RuntimeException) t;
            } else {
                throw new RuntimeException(t);
            }
        }
    }

    @Override
    public SyncObject reverseFilter(ObjectContext objectContext) {
        return getNext().reverseFilter(objectContext);
    }
}
