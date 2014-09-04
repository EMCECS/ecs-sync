/*
 * Copyright 2013 EMC Corporation. All Rights Reserved.
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
package com.emc.vipr.sync.util;

import com.emc.atmos.api.bean.Metadata;
import com.emc.vipr.sync.model.SyncObject;
import org.apache.log4j.LogMF;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public final class AtmosUtil {
    private static final Logger l4j = Logger.getLogger(AtmosUtil.class);

    public static List<Metadata> getRetentionMetadataForUpdate(SyncObject<?> object) {
        List<Metadata> list = new ArrayList<>();

        Boolean retentionEnabled = object.getMetadata().isRetentionEnabled();
        Date retentionEnd = object.getMetadata().getRetentionEndDate();

        if (retentionEnd != null) {
            LogMF.debug(l4j, "Retention {0} (OID: {1}, end-date: {2})", retentionEnabled ? "enabled" : "disabled",
                    object.getSourceIdentifier(), Iso8601Util.format(retentionEnd));
            list.add(new Metadata("user.maui.retentionEnable", retentionEnabled.toString(), false));
            list.add(new Metadata("user.maui.retentionEnd", Iso8601Util.format(retentionEnd), false));
        }

        return list;
    }

    public static List<Metadata> getExpirationMetadataForUpdate(SyncObject<?> object) {
        List<Metadata> list = new ArrayList<>();

        Boolean expirationEnabled = object.getMetadata().isExpirationEnabled();
        Date expiration = object.getMetadata().getExpirationDate();

        if (expiration != null) {
            LogMF.debug(l4j, "Expiration {0} (OID: {1}, end-date: {2})", expirationEnabled ? "enabled" : "disabled",
                    object.getSourceIdentifier(), Iso8601Util.format(expiration));
            list.add(new Metadata("user.maui.expirationEnable", expirationEnabled.toString(), false));
            list.add(new Metadata("user.maui.expirationEnd", Iso8601Util.format(expiration), false));
        }

        return list;
    }

    private AtmosUtil() {
    }
}
