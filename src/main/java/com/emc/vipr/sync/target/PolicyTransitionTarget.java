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
package com.emc.vipr.sync.target;

import com.emc.atmos.api.AtmosApi;
import com.emc.atmos.api.ObjectIdentifier;
import com.emc.atmos.api.ObjectPath;
import com.emc.atmos.api.bean.Metadata;
import com.emc.vipr.sync.filter.SyncFilter;
import com.emc.vipr.sync.model.AtmosMetadata;
import com.emc.vipr.sync.model.SyncObject;
import com.emc.vipr.sync.source.AtmosSource;
import com.emc.vipr.sync.source.SyncSource;
import com.emc.vipr.sync.util.AtmosUtil;
import com.emc.vipr.sync.util.ConfigurationException;
import com.emc.vipr.sync.util.OptionBuilder;
import com.emc.vipr.sync.util.Function;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.log4j.LogMF;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Intended to provide policy transitioning via metadata update with support for maintaining expiration/retention
 * dates. Use this plug-in in combination with AtmosSource. The objects in the source cloud will
 * be modified with the assumed intent of transitioning to a different policy.
 */
public class PolicyTransitionTarget extends SyncTarget {
    private static final Logger l4j = Logger.getLogger(PolicyTransitionTarget.class);

    public static final String TARGET_PATTERN = "^atmos-policy:(.*)$";

    public static final String META_TRIGGER_OPTION = "meta-trigger";
    public static final String META_TRIGGER_DESC = "Specifies the metadata (name=value) that will trigger the policy change";
    public static final String META_TRIGGER_ARG_NAME = "metadata";

    public static final String REMOVE_META_OPTION = "remove-meta";
    public static final String REMOVE_META_DESC = "If enabled, removes the trigger metadata after it has been set. Use this if the trigger metadata serves no other purpose than to change the policy";

    public static final String DISABLE_RETENTION_OPTION = "disable-retention";
    public static final String DISABLE_RETENTION_DESC = "If objects are currently in retention, this will *attempt* to disable it to allow the policy transition (otherwise it is impossible). Note: this will only work in non-compliant subtenants";

    public static final String KEEP_EXPIRATION_OPTION = "keep-expiration";
    public static final String KEEP_EXPIRATION_DESC = "When a new policy is applied, the expiration date may be changed. This option will pull the expiration date before the update and attempt to re-set the expiration after the update (via user.maui.expirationEnd)";

    public static final String KEEP_RETENTION_OPTION = "keep-retention";
    public static final String KEEP_RETENTION_DESC = "Same as keep-expiration except for retention end date";

    public static final String FAST_OPTION = "fast";
    public static final String FAST_DESC = "Skip all checking and verification and make only set-metadata (and delete-metadata if removing) calls";

    public static final String VERIFY_POLICY_OPTION = "verify-policy";
    public static final String VERIFY_POLICY_DESC = "Makes an additional call to verify the policy assignment for each object and logs success as INFO, failure as WARN. Note this does not retry if the policy assignment was unsuccessful";

    // timed operations
    private static final String OPERATION_DISABLE_RETENTION = "AtmosDisableRetention";
    private static final String OPERATION_SET_USER_META = "AtmosSetUserMeta";
    private static final String OPERATION_DELETE_USER_META = "AtmosDeleteUserMeta";
    private static final String OPERATION_SET_RETENTION_EXPIRATION = "AtmosSetRetentionExpiration";
    private static final String OPERATION_VERIFY_POLICY = "AtmosVerifyPolicy";
    private static final String OPERATION_TOTAL = "TotalTime";

    private String targetPolicy;
    private Metadata triggerMetadata;
    private boolean removeMeta;
    private boolean disableRetention;
    private boolean keepExpiration;
    private boolean keepRetention;
    private AtmosSource source;
    private boolean fast;
    private boolean verifyPolicy;

    @Override
    public boolean canHandleTarget(String targetUri) {
        return targetUri.matches(TARGET_PATTERN);
    }

    @Override
    public Options getCustomOptions() {
        Options opts = new Options();
        opts.addOption(new OptionBuilder().withLongOpt(META_TRIGGER_OPTION).withDescription(META_TRIGGER_DESC)
                .hasArg().withArgName(META_TRIGGER_ARG_NAME).create());
        opts.addOption(new OptionBuilder().withDescription(REMOVE_META_DESC).withLongOpt(REMOVE_META_OPTION).create());
        opts.addOption(new OptionBuilder().withDescription(DISABLE_RETENTION_DESC).withLongOpt(DISABLE_RETENTION_OPTION).create());
        opts.addOption(new OptionBuilder().withDescription(KEEP_EXPIRATION_DESC).withLongOpt(KEEP_EXPIRATION_OPTION).create());
        opts.addOption(new OptionBuilder().withDescription(KEEP_RETENTION_DESC).withLongOpt(KEEP_RETENTION_OPTION).create());
        opts.addOption(new OptionBuilder().withDescription(FAST_DESC).withLongOpt(FAST_OPTION).create());
        opts.addOption(new OptionBuilder().withDescription(VERIFY_POLICY_DESC).withLongOpt(VERIFY_POLICY_OPTION).create());
        return opts;
    }

    @Override
    protected void parseCustomOptions(CommandLine line) {
        Matcher m = Pattern.compile(TARGET_PATTERN).matcher(targetUri);
        if (!m.matches())
            throw new ConfigurationException("target option does not match pattern (how did this plug-in get loaded?)");

        targetPolicy = m.group(1);
        if (targetPolicy.isEmpty()) targetPolicy = null;

        if (line.hasOption(META_TRIGGER_OPTION)) {
            String[] parts = line.getOptionValue(META_TRIGGER_OPTION).split("=");
            if (parts.length != 2)
                throw new ConfigurationException("metadata trigger must be specified by \"name=value\"");

            triggerMetadata = new Metadata(parts[0], parts[1], false);
        }

        removeMeta = line.hasOption(REMOVE_META_OPTION);

        disableRetention = line.hasOption(DISABLE_RETENTION_OPTION);
        keepExpiration = line.hasOption(KEEP_EXPIRATION_OPTION);
        keepRetention = line.hasOption(KEEP_RETENTION_OPTION);
        verifyPolicy = line.hasOption(VERIFY_POLICY_OPTION);

        fast = line.hasOption(FAST_OPTION);
    }

    @Override
    public void configure(SyncSource source, Iterator<SyncFilter> filters, SyncTarget target) {
        if (triggerMetadata == null)
            throw new ConfigurationException("policy transition requires a metadata trigger");

        if (fast && (disableRetention || keepExpiration || keepRetention || verifyPolicy))
            throw new ConfigurationException("disable-retention, keep-retention, keep-expiration and verify-policy are not possible in fast mode");

        if (verifyPolicy && targetPolicy == null)
            throw new ConfigurationException("you must specify a target-policy to verify");

        if (source instanceof AtmosSource) {
            if ((keepExpiration || keepRetention) && !includeRetentionExpiration)
                throw new ConfigurationException("you must enable includeRetentionExpiration to keep retention or expiration dates");
            this.source = ((AtmosSource) source);
        } else {
            throw new ConfigurationException("PolicyTransitionTarget must be used solely with AtmosSource");
        }
    }

    @Override
    public void filter(final SyncObject obj) {
        timeOperationStart(OPERATION_TOTAL);

        final ObjectIdentifier id = (ObjectIdentifier) obj.getRawSourceIdentifier();

        // ignore directories
        if (id instanceof ObjectPath && ((ObjectPath) id).isDirectory()) {
            LogMF.debug(l4j, "Object {0} is a directory; ignoring", id);
            return;
        }

        // ignore the object if it's already in the target policy
        if (targetPolicy != null && !fast) {
            String policy = ((AtmosMetadata) obj.getMetadata()).getSystemMetadataValue("policyname");
            if (policy != null && targetPolicy.equals(policy)) {
                l4j.info("Object " + id + " is already in target policy " + targetPolicy + "; ignoring");
                return;
            }
        }

        final AtmosApi atmosApi = source.getAtmos();
        try {
            if (!fast) obj.getMetadata(); // this will lazy-load metadata and object-info

            // check if the object is already in retention. if so, we'll need to disable retention before we can change
            // the metadata
            if (disableRetention && ((AtmosMetadata) obj.getMetadata()).isRetentionEnabled()) {
                time(new Function<Void>() {
                    @Override
                    public Void call() {
                        atmosApi.setUserMetadata(id, new Metadata("user.maui.retentionEnable", "false", false));
                        return null;
                    }
                }, OPERATION_DISABLE_RETENTION);
            }

            // change policy (update metadata)
            time(new Function<Void>() {
                @Override
                public Void call() {
                    atmosApi.setUserMetadata(id, triggerMetadata);
                    return null;
                }
            }, OPERATION_SET_USER_META);

            // verify policy assignment
            if (verifyPolicy) {
                int maxTries = 3, thisTry = 1;
                Metadata policyMeta = null;
                boolean success = false;
                try {
                    do {
                        // if not immediately set, give the policy trigger some time to take effect
                        if (thisTry > 1) Thread.sleep(300);
                        policyMeta = time(new Function<Metadata>() {
                            @Override
                            public Metadata call() {
                                return atmosApi.getSystemMetadata(id, "policyname").get("policyname");
                            }
                        }, OPERATION_VERIFY_POLICY);
                        success = policyMeta != null && targetPolicy.equals(policyMeta.getValue());
                    } while (!success && ++thisTry <= maxTries);
                } catch (InterruptedException e) {
                    l4j.warn("Interrupted while waiting to retry policy verification");
                }
                if (success) {
                    LogMF.info(l4j, "Object {0} successfully transitioned to target policy {1}", id, targetPolicy);
                } else {
                    LogMF.warn(l4j, "! Object {0} transition unsuccessful ({1})", id, policyMeta);
                }
            }

            // if keeping retention or deletion, re-set those to the old values
            final List<Metadata> retExpList = new ArrayList<Metadata>();

            if (keepExpiration)
                retExpList.addAll(AtmosUtil.getExpirationMetadataForUpdate(obj));

            if (keepRetention)
                retExpList.addAll(AtmosUtil.getRetentionMetadataForUpdate(obj));

            if (retExpList.size() > 0) {
                time(new Function<Void>() {
                    @Override
                    public Void call() {
                        atmosApi.setUserMetadata(id, retExpList.toArray(new Metadata[retExpList.size()]));
                        return null;
                    }
                }, OPERATION_SET_RETENTION_EXPIRATION);
            }

            // remove metadata after transition
            if (removeMeta) {
                time(new Function<Object>() {
                    @Override
                    public Object call() {
                        atmosApi.deleteUserMetadata(id, triggerMetadata.getName());
                        return null;
                    }
                }, OPERATION_DELETE_USER_META);
            }

            getNext().filter(obj);

            timeOperationComplete(OPERATION_TOTAL);
        } catch (RuntimeException e) {
            timeOperationFailed(OPERATION_TOTAL);
            throw e;
        }
    }

    @Override
    public String getName() {
        return "Policy Transition Target";
    }

    @Override
    public String getDocumentation() {
        return "The policy transition target transitions objects to a different policy via metadata update with " +
                "support for maintaining expiration/retention dates. Use AtmosSource " +
                "and a target of:\n    atmos-policy:[policy-name]\n" +
                "The objects in the source cloud will be modified with the assumed intent of " +
                "transitioning to a different policy. If a policy-name is specified, each object is checked to verify " +
                "that it is not already in the target policy (skipped objects are logged at INFO level)";
    }

    public String getTargetPolicy() {
        return targetPolicy;
    }

    /**
     * The target policy in which objects should be placed. If specified, the system metadata for each object is
     * checked to verify the object is not already in the target policy. If so, it is left untouched.
     */
    public void setTargetPolicy(String targetPolicy) {
        this.targetPolicy = targetPolicy;
    }

    public Metadata getTriggerMetadata() {
        return triggerMetadata;
    }

    /**
     * Specifies the metadata that will trigger the policy change. Required.
     */
    public void setTriggerMetadata(Metadata triggerMetadata) {
        this.triggerMetadata = triggerMetadata;
    }

    public boolean isRemoveMeta() {
        return removeMeta;
    }

    /**
     * If true, removes the trigger metadata after it has been set. Use this if the trigger metadata serves no other
     * purpose than to change the policy. Default is false.
     */
    public void setRemoveMeta(boolean removeMeta) {
        this.removeMeta = removeMeta;
    }

    public boolean isKeepExpiration() {
        return keepExpiration;
    }

    /**
     * When a new policy is applied, the expiration date may be changed. When set to true, the expiration date is
     * pulled before the update and an attempt is made to re-set the expiration after the update (via
     * user.maui.expirationEnd). Default is false.
     */
    public void setKeepExpiration(boolean keepExpiration) {
        this.keepExpiration = keepExpiration;
    }

    public boolean isKeepRetention() {
        return keepRetention;
    }

    /**
     * Same as keepExpiration except for retention end date
     */
    public void setKeepRetention(boolean keepRetention) {
        this.keepRetention = keepRetention;
    }

    public boolean isFast() {
        return fast;
    }

    /**
     * If true, skips all checking and validation and makes only set-metadata (and delete-metadata if removing) calls.
     */
    public void setFast(boolean fast) {
        this.fast = fast;
    }

    /**
     * If true, makes an additional call to verify the policy assignment for each object and logs the result at INFO
     * level. Note this does not retry if the policy assignment was unsuccessful.
     */
    public boolean isVerifyPolicy() {
        return verifyPolicy;
    }

    public void setVerifyPolicy(boolean verifyPolicy) {
        this.verifyPolicy = verifyPolicy;
    }
}
