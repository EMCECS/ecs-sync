package com.emc.ecs.sync.filter;

import com.emc.ecs.sync.model.object.ClamAvSyncObject;
import com.emc.ecs.sync.model.object.SyncObject;
import com.emc.ecs.sync.source.SyncSource;
import com.emc.ecs.sync.target.SyncTarget;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.Socket;
import java.util.Iterator;

/**
 * Uses ClamAV to scan the stream for viruses.  If a virus is detected, the stream will be terminated immediately,
 * potentially mid-stream.
 *
 */
public class ClamAvFilter extends SyncFilter {
    private static final Logger log = LoggerFactory.getLogger(ClamAvFilter.class);

    public static final String ACTIVATION_NAME = "virus-scan";

    public static final String CLAMD_HOST_OPTION = "clamd-host";
    public static final String CLAMD_HOST_DESC = "The hostname of the server running clamd.  Defaults to localhost.";
    public static final String CLAMD_HOST_ARG_NAME = "hostname";

    public static final String CLAMD_PORT_OPTION = "clamd-port";
    public static final String CLAMD_PORT_DESC = "The port number running clamd.  Defaults to 3310";
    public static final String CLAMD_POST_ARG_NAME = "port";

    public static final String CLAMD_MAX_SIZE_OPTION = "clamd-max-size";
    public static final String CLAMD_MAX_SIZE_DESC = "Maximum stream size accepted by clamd.  Defaults to 25MB (the " +
            "clamd default)";
    public static final String CLAMD_MAX_SIZE_ARG_NAME = "bytes";

    private String host = "localhost";
    private int port = 3310;
    private long maxScanSize = 25*1024*1024;

    @Override
    public Options getCustomOptions() {
        Options opts = new Options();

        opts.addOption(Option.builder().longOpt(CLAMD_HOST_OPTION).desc(CLAMD_HOST_DESC).hasArg().argName(CLAMD_HOST_ARG_NAME).build());
        opts.addOption(Option.builder().longOpt(CLAMD_PORT_OPTION).desc(CLAMD_PORT_DESC).hasArg().argName(CLAMD_POST_ARG_NAME).build());
        opts.addOption(Option.builder().longOpt(CLAMD_MAX_SIZE_OPTION).desc(CLAMD_MAX_SIZE_DESC).hasArg().argName(CLAMD_MAX_SIZE_ARG_NAME).build());

        return opts;
    }

    @Override
    public String getActivationName() {
        return ACTIVATION_NAME;
    }

    @Override
    public void filter(SyncObject obj) {
        getNext().filter(new ClamAvSyncObject(obj, host, port, maxScanSize));
    }

    @Override
    public SyncObject reverseFilter(SyncObject obj) {
        return getNext().reverseFilter(obj);
    }


    @Override
    public String getName() {
        return "Virus Scan";
    }

    @Override
    public String getDocumentation() {
        return "Connects to a ClamAV daemon to scan the stream for viruses.  If a virus is found, the stream will be " +
                "terminated immediately, possibly mid-stream";
    }


    @Override
    protected void parseCustomOptions(CommandLine line) {
        if(line.hasOption(CLAMD_HOST_OPTION)) {
            host = line.getOptionValue(CLAMD_HOST_OPTION);
        }
        if(line.hasOption(CLAMD_PORT_OPTION)) {
            port = Integer.parseInt(line.getOptionValue(CLAMD_PORT_OPTION));
        }
        if(line.hasOption(CLAMD_MAX_SIZE_OPTION)) {
            maxScanSize = Long.parseLong(line.getOptionValue(CLAMD_MAX_SIZE_OPTION));
        }
    }

    @Override
    public void configure(SyncSource source, Iterator<SyncFilter> filters, SyncTarget target) {
        // Make sure we can connect to clamd
        clamPing(host, port);
    }

    /**
     * Makes sure clamd is running on the given host/port.
     * @param host host or IP to check
     * @param port port to check
     */
    public static void clamPing(String host, int port) {
        try {
            Socket sock = new Socket(host, port);

            BufferedReader br = new BufferedReader(new InputStreamReader(sock.getInputStream()));
            DataOutputStream out = new DataOutputStream(sock.getOutputStream());

            out.write("PING\n".getBytes("US-ASCII"));
            String line = br.readLine();

            if(!line.equals("PONG")) {
                throw new RuntimeException("Unexpected server response: " + line);
            }

            br.close();
            out.close();
            sock.close();

            log.info("ClamAV connection test succeeded.");

        } catch(Exception e) {
            throw new RuntimeException("Failed to test ClamAV", e);
        }
    }
}
