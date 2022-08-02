/*
 * Copyright (c) 2021-2022 Dell Inc. or its subsidiaries. All Rights Reserved.
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

import com.emc.ecs.sync.config.ConfigurationException;
import com.emc.ecs.sync.config.filter.PathMappingConfig;
import com.emc.ecs.sync.model.ObjectContext;
import com.emc.ecs.sync.model.SyncObject;
import com.emc.ecs.sync.storage.SyncStorage;
import org.apache.commons.csv.CSVRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import static com.emc.ecs.sync.config.filter.PathMappingConfig.MapSource.*;

public class PathMappingFilter extends AbstractFilter<PathMappingConfig> {
    private static final Logger log = LoggerFactory.getLogger(PathMappingFilter.class);

    private Pattern pattern;
    private SyncStorage<?> sourceStorage;

    @Override
    public void configure(SyncStorage<?> source, Iterator<? extends SyncFilter<?>> filters, SyncStorage<?> target) {
        super.configure(source, filters, target);
        this.sourceStorage = source;

        if (config.getMapSource() == CSV) {
            if (options.getSourceListFile() == null) {
                throw new ConfigurationException("MapSource set to CSV, but no source file provided");
            }
        }

        if (config.getMapSource() == Metadata) {
            if (config.getMetadataName() == null) {
                throw new ConfigurationException("MapSource set to Metadata, but no metadata name set");
            }
        }

        if (config.getMapSource() == RegEx) {
            if (config.getRegExPattern() == null || config.getRegExReplacementString() == null) {
                throw new ConfigurationException("MapSource set to RegEx, but regExPattern and regExReplacementString are not set");
            }
            try {
                pattern = Pattern.compile(config.getRegExPattern());
            } catch (PatternSyntaxException e) {
                throw new ConfigurationException("Bad RegEx pattern", e);
            }
        }
    }

    @Override
    public void filter(ObjectContext objectContext) {
        SyncObject sourceObject = objectContext.getObject();
        String sourceId = objectContext.getSourceSummary().getIdentifier();
        String originalPath = sourceObject.getRelativePath();
        String newPath = null;

        // pull from 2nd column of source-list-file CSV
        if (config.getMapSource() == CSV) {
            String listFileRow = objectContext.getSourceSummary().getListFileRow();
            if (listFileRow == null)
                throw new RuntimeException("No list file data for " + sourceId + " (are you using the recursive option?)");
            CSVRecord fileRecord = getListFileCsvRecord(listFileRow);
            if (fileRecord.size() < 2)
                throw new RuntimeException("No target mapping for " + sourceId + " in CSV file");
            newPath = fileRecord.get(1);
            log.debug("found path mapping in CSV: {} => {}", sourceId, newPath);

        } else if (config.getMapSource() == Metadata) {
            newPath = sourceObject.getMetadata().getUserMetadataValue(config.getMetadataName());
            if (newPath == null)
                throw new RuntimeException(String.format("No target mapping for %s (no metadata value for %s)",
                        sourceId, config.getMetadataName()));
            log.debug("found path mapping in metadata: {}[{}] = {}", sourceId, config.getMetadataName(), newPath);

        } else if (config.getMapSource() == RegEx) {
            newPath = pattern.matcher(originalPath).replaceAll(config.getRegExReplacementString());
            log.debug("applied regex replacement to path: {} => {}", originalPath, newPath);
        }

        log.info("mapping {} => {}", originalPath, newPath);

        /*
        TODO: need to make sure swapping out the SyncObject in ObjectContext doesn't have any negative impacts
              (we also do this in *Extractor filters)
        TODO: need to rethink the SyncObject abstraction - maybe we should have a source object that is immutable and
              a target object that is mutable and separate - both could be referenced in ObjectContext and the target
              object is what gets written to target storage
         */
        SyncObject mappedObject = new SyncObject(sourceStorage, newPath, sourceObject.getMetadata(),
                sourceObject.getDataStream(), sourceObject.getAcl()) {
            // make sure we close the original object
            @Override
            public void close() throws Exception {
                sourceObject.close();
                super.close();
            }
        };
        objectContext.setObject(mappedObject);

        if (config.isStorePreviousPathAsMetadata()) {
            log.debug("storing original path as object metadata ({}: {})",
                    PathMappingConfig.META_PREVIOUS_NAME, originalPath);
            mappedObject.getMetadata().setUserMetadataValue(PathMappingConfig.META_PREVIOUS_NAME, originalPath);
        }

        getNext().filter(objectContext);
    }

    @Override
    public SyncObject reverseFilter(ObjectContext objectContext) {
        return getNext().reverseFilter(objectContext);
    }
}
