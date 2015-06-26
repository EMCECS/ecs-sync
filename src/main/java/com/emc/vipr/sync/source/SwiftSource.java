package com.emc.vipr.sync.source;

import com.amazonaws.services.s3.AmazonS3;
import com.emc.vipr.sync.filter.SyncFilter;
import com.emc.vipr.sync.model.object.S3SyncObject;
import com.emc.vipr.sync.model.object.SwiftSyncObject;
import com.emc.vipr.sync.target.S3Target;
import com.emc.vipr.sync.target.SyncTarget;
import com.emc.vipr.sync.util.OptionBuilder;
import com.emc.vipr.sync.util.SwiftUtil;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.log4j.LogMF;
import org.javaswift.joss.swift.Swift;

import java.util.Iterator;
import java.util.logging.Logger;

/**
 * Created by joshia7 on 6/26/15.
 */
public class SwiftSource extends SyncSource<SwiftSyncObject>{

    private static final Logger l4j = Logger.getLogger(String.valueOf(SwiftSource.class));

    public static final String CONTAINER_OPTION = "source-container";
    public static final String CONTAINER_DESC = "Required. Specifies the source bucket to use.";
    public static final String CONTAINER_ARG_NAME = "container";

    public static final String DECODE_KEYS_OPTION = "source-decode-keys";
    public static final String DECODE_KEYS_DESC = "If specified, keys will be URL-decoded after listing them.  This can fix problems if you see file or directory names with characters like %2f in them.";

    public static final String DISABLE_VHOSTS_OPTION = "source-disable-vhost";
    public static final String DISABLE_VHOSTS_DESC = "If specified, virtual hosted buckets will be disabled and path-style buckets will be used.";

    public static final String OPERATION_DELETE_OBJECT = "SwiftDeleteObject";

    private String protocol;
    private String endpoint;
    private String username;
    private String password;
    private boolean disableVHosts;
    private String containerName;
    private String rootKey;
    private boolean decodeKeys;
    private S3Target s3Target;
    private boolean versioningEnabled = false;

    private Swift swift;



    @Override
    public boolean canHandleSource(String sourceUri)
        {
            return sourceUri.startsWith(SwiftUtil.URI_PREFIX);
        }




    @Override
    public Iterator<SwiftSyncObject> childIterator(SwiftSyncObject syncObject) {
        return null;
    }

    @Override
    public Iterator<SwiftSyncObject> iterator() {
        return null;
    }

    @Override
    public String getName() {
        return null;
    }

    @Override
    public String getDocumentation() {
        return null;
    }

    @Override
    public Options getCustomOptions() {
        Options opts = new Options();
        opts.addOption(new OptionBuilder().withLongOpt(CONTAINER_OPTION).withDescription(CONTAINER_DESC)
                .hasArg().withArgName(CONTAINER_ARG_NAME).create());
        opts.addOption(new OptionBuilder().withLongOpt(DECODE_KEYS_OPTION).withDescription(DECODE_KEYS_DESC).create());
        opts.addOption(new OptionBuilder().withLongOpt(DISABLE_VHOSTS_OPTION).withDescription(DISABLE_VHOSTS_DESC).create());
        return opts;
    }

    @Override
    protected void parseCustomOptions(CommandLine line) {

        SwiftUtil.SwiftUri s3Uri = SwiftUtil.parseUri(sourceUri);
        protocol = s3Uri.protocol;
        endpoint = s3Uri.endpoint;
        username = s3Uri.username;
        password = s3Uri.password;
        rootKey = s3Uri.rootKey;

        if (line.hasOption(CONTAINER_OPTION))
            containerName = line.getOptionValue(CONTAINER_OPTION);

        disableVHosts = line.hasOption(DISABLE_VHOSTS_OPTION);

        decodeKeys = line.hasOption(DECODE_KEYS_OPTION);

    }

    @Override
    public void configure(SyncSource source, Iterator<SyncFilter> filters, SyncTarget target) {

    }
}
