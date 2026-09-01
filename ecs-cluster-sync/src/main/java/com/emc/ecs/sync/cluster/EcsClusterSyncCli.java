/*
 * Copyright (c) 2024 Dell Inc. or its subsidiaries. All Rights Reserved.
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
package com.emc.ecs.sync.cluster;

import com.emc.ecs.sync.config.Protocol;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.bind.JAXBContext;
import java.io.File;
import java.io.PrintWriter;

/**
 * Entry point for whole-cluster replication:
 * <pre>
 * java -cp ecs-sync-&lt;version&gt;.jar com.emc.ecs.sync.cluster.EcsClusterSyncCli \
 *     --source 10.0.0.1 --source-user root --source-password ... \
 *     --target 10.1.0.1 --target-user root --target-password ...
 * </pre>
 */
public class EcsClusterSyncCli {
    private static final Logger log = LoggerFactory.getLogger(EcsClusterSyncCli.class);

    private static final String OPT_XML_CONFIG = "xml-config";
    private static final String OPT_SOURCE = "source";
    private static final String OPT_SOURCE_USER = "source-user";
    private static final String OPT_SOURCE_PASSWORD = "source-password";
    private static final String OPT_SOURCE_MGMT_PORT = "source-mgmt-port";
    private static final String OPT_SOURCE_DATA_PORT = "source-data-port";
    private static final String OPT_SOURCE_DATA_HOSTS = "source-data-hosts";
    private static final String OPT_SOURCE_DATA_PROTOCOL = "source-data-protocol";
    private static final String OPT_TARGET = "target";
    private static final String OPT_TARGET_USER = "target-user";
    private static final String OPT_TARGET_PASSWORD = "target-password";
    private static final String OPT_TARGET_MGMT_PORT = "target-mgmt-port";
    private static final String OPT_TARGET_DATA_PORT = "target-data-port";
    private static final String OPT_TARGET_DATA_HOSTS = "target-data-hosts";
    private static final String OPT_TARGET_DATA_PROTOCOL = "target-data-protocol";
    private static final String OPT_TRUST_SELF_SIGNED = "trust-self-signed";
    private static final String OPT_INCLUDE_NAMESPACES = "include-namespaces";
    private static final String OPT_EXCLUDE_NAMESPACES = "exclude-namespaces";
    private static final String OPT_TARGET_RG = "target-replication-group";
    private static final String OPT_TARGET_NS_SUFFIX = "target-namespace-suffix";
    private static final String OPT_NAMESPACE_MAP = "namespace-map";
    private static final String OPT_SKIP_NAMESPACES = "skip-namespace-creation";
    private static final String OPT_SKIP_USERS = "skip-user-creation";
    private static final String OPT_SKIP_BUCKETS = "skip-bucket-creation";
    private static final String OPT_NO_SOURCE_KEY_GEN = "no-source-key-generation";
    private static final String OPT_THREADS = "threads";
    private static final String OPT_BUCKET_PARALLELISM = "bucket-parallelism";
    private static final String OPT_VERIFY = "verify";
    private static final String OPT_NO_VERSIONS = "no-versions";
    private static final String OPT_DRY_RUN = "dry-run";
    private static final String OPT_STRUCTURE_ONLY = "structure-only";
    private static final String OPT_DB_CONNECT_STRING = "db-connect-string";
    private static final String OPT_DB_ENC_PASSWORD = "db-enc-password";
    private static final String OPT_DB_FILE = "db-file";
    private static final String OPT_HELP = "help";

    public static void main(String[] args) {
        Options options = createOptions();
        try {
            CommandLine commandLine = new DefaultParser().parse(options, args);

            if (commandLine.hasOption(OPT_HELP) || args.length == 0) {
                printHelp(options);
                System.exit(0);
            }

            ClusterSyncConfig config = commandLine.hasOption(OPT_XML_CONFIG)
                    ? loadXml(new File(commandLine.getOptionValue(OPT_XML_CONFIG)))
                    : buildConfig(commandLine);

            ClusterSyncReport report;
            try (ClusterReplicator replicator = new ClusterReplicator(config)) {
                report = replicator.run();
            }

            log.info("cluster replication summary:{}{}", System.lineSeparator(), report.summarize());
            System.exit(report.isSuccessful() ? 0 : 2);
        } catch (ParseException e) {
            System.err.println("error: " + e.getMessage());
            printHelp(options);
            System.exit(1);
        } catch (Exception e) {
            log.error("cluster replication failed: {}", e.toString());
            log.debug("stacktrace:", e);
            System.exit(1);
        }
    }

    static ClusterSyncConfig loadXml(File file) throws Exception {
        return (ClusterSyncConfig) JAXBContext.newInstance(ClusterSyncConfig.class)
                .createUnmarshaller().unmarshal(file);
    }

    static ClusterSyncConfig buildConfig(CommandLine cmd) throws ParseException {
        requireOption(cmd, OPT_SOURCE);
        requireOption(cmd, OPT_SOURCE_USER);
        requireOption(cmd, OPT_SOURCE_PASSWORD);
        requireOption(cmd, OPT_TARGET);
        requireOption(cmd, OPT_TARGET_USER);
        requireOption(cmd, OPT_TARGET_PASSWORD);

        boolean trustSelfSigned = cmd.hasOption(OPT_TRUST_SELF_SIGNED);

        ClusterEndpoint source = new ClusterEndpoint();
        source.setManagementEndpoint(cmd.getOptionValue(OPT_SOURCE));
        source.setAdminUsername(cmd.getOptionValue(OPT_SOURCE_USER));
        source.setAdminPassword(cmd.getOptionValue(OPT_SOURCE_PASSWORD));
        source.setManagementPort(intOption(cmd, OPT_SOURCE_MGMT_PORT, ClusterEndpoint.DEFAULT_MANAGEMENT_PORT));
        source.setDataPort(intOption(cmd, OPT_SOURCE_DATA_PORT, ClusterEndpoint.DEFAULT_DATA_PORT));
        source.setTrustSelfSignedCertificates(trustSelfSigned);
        if (cmd.hasOption(OPT_SOURCE_DATA_HOSTS))
            source.setDataEndpoints(cmd.getOptionValue(OPT_SOURCE_DATA_HOSTS).split(","));
        if (cmd.hasOption(OPT_SOURCE_DATA_PROTOCOL))
            source.setDataProtocol(Protocol.valueOf(cmd.getOptionValue(OPT_SOURCE_DATA_PROTOCOL).toLowerCase()));

        ClusterEndpoint target = new ClusterEndpoint();
        target.setManagementEndpoint(cmd.getOptionValue(OPT_TARGET));
        target.setAdminUsername(cmd.getOptionValue(OPT_TARGET_USER));
        target.setAdminPassword(cmd.getOptionValue(OPT_TARGET_PASSWORD));
        target.setManagementPort(intOption(cmd, OPT_TARGET_MGMT_PORT, ClusterEndpoint.DEFAULT_MANAGEMENT_PORT));
        target.setDataPort(intOption(cmd, OPT_TARGET_DATA_PORT, ClusterEndpoint.DEFAULT_DATA_PORT));
        target.setTrustSelfSignedCertificates(trustSelfSigned);
        if (cmd.hasOption(OPT_TARGET_DATA_HOSTS))
            target.setDataEndpoints(cmd.getOptionValue(OPT_TARGET_DATA_HOSTS).split(","));
        if (cmd.hasOption(OPT_TARGET_DATA_PROTOCOL))
            target.setDataProtocol(Protocol.valueOf(cmd.getOptionValue(OPT_TARGET_DATA_PROTOCOL).toLowerCase()));

        ClusterSyncOptions options = new ClusterSyncOptions();
        if (cmd.hasOption(OPT_INCLUDE_NAMESPACES))
            options.setIncludeNamespaces(cmd.getOptionValue(OPT_INCLUDE_NAMESPACES).split(","));
        if (cmd.hasOption(OPT_EXCLUDE_NAMESPACES))
            options.setExcludeNamespaces(cmd.getOptionValue(OPT_EXCLUDE_NAMESPACES).split(","));
        options.setTargetReplicationGroup(cmd.getOptionValue(OPT_TARGET_RG));
        if (cmd.hasOption(OPT_TARGET_NS_SUFFIX))
            options.setTargetNamespaceSuffix(cmd.getOptionValue(OPT_TARGET_NS_SUFFIX));
        if (cmd.hasOption(OPT_NAMESPACE_MAP))
            options.setNamespaceMap(cmd.getOptionValues(OPT_NAMESPACE_MAP));
        options.setSkipNamespaceCreation(cmd.hasOption(OPT_SKIP_NAMESPACES));
        options.setSkipUserCreation(cmd.hasOption(OPT_SKIP_USERS));
        options.setSkipBucketCreation(cmd.hasOption(OPT_SKIP_BUCKETS));
        options.setGenerateSourceSecretKeys(!cmd.hasOption(OPT_NO_SOURCE_KEY_GEN));
        options.setThreadCount(intOption(cmd, OPT_THREADS, ClusterSyncOptions.DEFAULT_THREAD_COUNT));
        options.setBucketParallelism(
                intOption(cmd, OPT_BUCKET_PARALLELISM, ClusterSyncOptions.DEFAULT_BUCKET_PARALLELISM));
        options.setVerify(cmd.hasOption(OPT_VERIFY));
        options.setIncludeVersions(!cmd.hasOption(OPT_NO_VERSIONS));
        options.setDryRun(cmd.hasOption(OPT_DRY_RUN));
        options.setStructureOnly(cmd.hasOption(OPT_STRUCTURE_ONLY));
        options.setDbConnectString(cmd.getOptionValue(OPT_DB_CONNECT_STRING));
        options.setDbEncPassword(cmd.getOptionValue(OPT_DB_ENC_PASSWORD));
        options.setDbFile(cmd.getOptionValue(OPT_DB_FILE));

        ClusterSyncConfig config = new ClusterSyncConfig();
        config.setSource(source);
        config.setTarget(target);
        config.setOptions(options);
        return config;
    }

    private static void requireOption(CommandLine cmd, String option) throws ParseException {
        if (!cmd.hasOption(option)) throw new ParseException("missing required option --" + option);
    }

    private static int intOption(CommandLine cmd, String option, int defaultValue) throws ParseException {
        if (!cmd.hasOption(option)) return defaultValue;
        try {
            return Integer.parseInt(cmd.getOptionValue(option));
        } catch (NumberFormatException e) {
            throw new ParseException("--" + option + " must be a number");
        }
    }

    private static Options createOptions() {
        Options options = new Options();
        options.addOption(arg(OPT_XML_CONFIG, "path to a clusterSyncConfig XML file (instead of the options below)"));

        options.addOption(arg(OPT_SOURCE, "source cluster management IP or hostname"));
        options.addOption(arg(OPT_SOURCE_USER, "source cluster management admin user"));
        options.addOption(arg(OPT_SOURCE_PASSWORD, "source cluster management admin password"));
        options.addOption(arg(OPT_SOURCE_MGMT_PORT,
                "source management port (default " + ClusterEndpoint.DEFAULT_MANAGEMENT_PORT + ")"));
        options.addOption(arg(OPT_SOURCE_DATA_PORT,
                "source S3 data port (default " + ClusterEndpoint.DEFAULT_DATA_PORT + ")"));
        options.addOption(arg(OPT_SOURCE_DATA_HOSTS,
                "comma-separated source S3 data nodes (default: the management IP)"));
        options.addOption(arg(OPT_SOURCE_DATA_PROTOCOL, "source S3 protocol, http or https (default http)"));

        options.addOption(arg(OPT_TARGET, "target cluster management IP or hostname"));
        options.addOption(arg(OPT_TARGET_USER, "target cluster management admin user"));
        options.addOption(arg(OPT_TARGET_PASSWORD, "target cluster management admin password"));
        options.addOption(arg(OPT_TARGET_MGMT_PORT,
                "target management port (default " + ClusterEndpoint.DEFAULT_MANAGEMENT_PORT + ")"));
        options.addOption(arg(OPT_TARGET_DATA_PORT,
                "target S3 data port (default " + ClusterEndpoint.DEFAULT_DATA_PORT + ")"));
        options.addOption(arg(OPT_TARGET_DATA_HOSTS,
                "comma-separated target S3 data nodes (default: the management IP)"));
        options.addOption(arg(OPT_TARGET_DATA_PROTOCOL, "target S3 protocol, http or https (default http)"));

        options.addOption(flag(OPT_TRUST_SELF_SIGNED,
                "accept self-signed certificates on both management endpoints"));
        options.addOption(arg(OPT_INCLUDE_NAMESPACES, "only replicate these namespaces (comma-separated)"));
        options.addOption(arg(OPT_EXCLUDE_NAMESPACES, "do not replicate these namespaces (comma-separated)"));
        options.addOption(arg(OPT_TARGET_RG,
                "name of the replication group to use on the target (default: match the source group by name)"));
        options.addOption(arg(OPT_TARGET_NS_SUFFIX,
                "suffix used when a namespace name is already taken on the target; a counter is appended if the "
                        + "suffixed name is taken too. Object users in a renamed namespace get the same suffix "
                        + "(default " + ClusterSyncOptions.DEFAULT_TARGET_NAMESPACE_SUFFIX + ")"));
        Option namespaceMap = Option.builder().longOpt(OPT_NAMESPACE_MAP).hasArgs().valueSeparator(',')
                .desc("explicit source=target namespace names, comma-separated or repeated. Bypasses conflict "
                        + "detection - use this to resume a previous run deterministically").build();
        options.addOption(namespaceMap);
        options.addOption(flag(OPT_SKIP_NAMESPACES, "do not create namespaces on the target"));
        options.addOption(flag(OPT_SKIP_USERS, "do not create object users on the target"));
        options.addOption(flag(OPT_SKIP_BUCKETS, "do not create buckets on the target"));
        options.addOption(flag(OPT_NO_SOURCE_KEY_GEN,
                "fail instead of generating a secret key on the source for users that have none"));

        options.addOption(arg(OPT_THREADS,
                "threads per bucket copy (default " + ClusterSyncOptions.DEFAULT_THREAD_COUNT + ")"));
        options.addOption(arg(OPT_BUCKET_PARALLELISM,
                "buckets copied concurrently (default " + ClusterSyncOptions.DEFAULT_BUCKET_PARALLELISM + ")"));
        options.addOption(flag(OPT_VERIFY, "verify each object after it is written"));
        options.addOption(flag(OPT_NO_VERSIONS, "copy only current versions of objects in versioned buckets"));
        options.addOption(flag(OPT_DRY_RUN, "report what would be replicated; change nothing"));
        options.addOption(flag(OPT_STRUCTURE_ONLY, "create namespaces, users and buckets but copy no objects"));

        options.addOption(arg(OPT_DB_CONNECT_STRING, "JDBC URL of a MySQL database for per-object status tracking"));
        options.addOption(arg(OPT_DB_ENC_PASSWORD, "encrypted password for the tracking database"));
        options.addOption(arg(OPT_DB_FILE, "SQLite file for per-object status tracking"));
        options.addOption(flag(OPT_HELP, "show this message"));
        return options;
    }

    private static Option arg(String name, String description) {
        return Option.builder().longOpt(name).hasArg().desc(description).build();
    }

    private static Option flag(String name, String description) {
        return Option.builder().longOpt(name).desc(description).build();
    }

    private static void printHelp(Options options) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.setWidth(120);
        formatter.setOptionComparator(null);
        PrintWriter writer = new PrintWriter(System.out);
        formatter.printHelp(writer, 120,
                "java -cp ecs-sync-<version>.jar " + EcsClusterSyncCli.class.getName() + " [options]",
                "\nReplicates an ECS cluster's tenancy and data: namespaces, then object users, then buckets"
                        + " (owned by the replicated user), then all objects.\n"
                        + "\nIf a namespace name is already taken on the target it is created with a suffix"
                        + " (ns1 -> ns1_target) and its object users are renamed to match. Because every namespace"
                        + " is therefore unique, the source and target may be the SAME cluster - which is how you"
                        + " migrate between storage pools.\n\nOptions:",
                options, 2, 4,
                "\nExample - between two clusters:\n"
                        + "  java -cp ecs-sync-<version>.jar " + EcsClusterSyncCli.class.getName() + " \\\n"
                        + "    --source 10.0.0.1 --source-user root --source-password 'pw1' \\\n"
                        + "    --target 10.1.0.1 --target-user root --target-password 'pw2' \\\n"
                        + "    --trust-self-signed --verify\n"
                        + "\nExample - storage pool migration within one cluster:\n"
                        + "  java -cp ecs-sync-<version>.jar " + EcsClusterSyncCli.class.getName() + " \\\n"
                        + "    --source 10.0.0.1 --source-user root --source-password 'pw' \\\n"
                        + "    --target 10.0.0.1 --target-user root --target-password 'pw' \\\n"
                        + "    --target-replication-group 'rg-on-new-pool' \\\n"
                        + "    --trust-self-signed --dry-run\n"
                        + "\nExit codes: 0 = success, 1 = fatal error, 2 = completed with per-bucket failures\n");
        writer.flush();
    }
}
