package com.emc.ecs.sync;

import com.emc.ecs.sync.config.ConfigUtil;
import com.emc.ecs.sync.config.ConfigWrapper;
import com.emc.ecs.sync.config.ConfigurationException;
import com.emc.ecs.sync.config.SyncConfig;
import com.emc.ecs.sync.rest.RestServer;
import com.emc.ecs.sync.service.SyncJobService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import java.io.File;
import java.util.ArrayList;
import java.util.List;

public class EcsSyncCli {
    private static final Logger log = LoggerFactory.getLogger(EcsSyncCli.class);

    public static final String VERSION = EcsSync.class.getPackage().getImplementationVersion();

    public static void main(String[] args) {
        int exitCode = 0;

        System.out.println(versionLine());

        RestServer restServer = null;
        try {

            // first, hush up the JDK logger (why does this default to INFO??)
            java.util.logging.LogManager.getLogManager().getLogger("").setLevel(java.util.logging.Level.WARNING);

            CliConfig cliConfig = CliHelper.parseCliConfig(args);

            if (cliConfig != null) {

                // configure logging for startup
                if (cliConfig.getLogLevel() != null)
                    LoggingUtil.setRootLogLevel(cliConfig.getLogLevel());

                // start REST service
                if (cliConfig.isRestEnabled()) {
                    if (cliConfig.getRestEndpoint() != null) {
                        String[] endpoint = cliConfig.getRestEndpoint().split(":");
                        restServer = new RestServer(endpoint[0], Integer.parseInt(endpoint[1]));
                    } else {
                        restServer = new RestServer();
                        restServer.setAutoPortEnabled(true);
                    }
                    // set DB connect string if provided
                    if (cliConfig.getDbConnectString() != null) {
                        SyncJobService.getInstance().setDbConnectString(cliConfig.getDbConnectString());
                    }
                    // set encrypted DB password if provided
                    if (cliConfig.getDbEncPassword() != null) {
                        SyncJobService.getInstance().setDbEncPassword(cliConfig.getDbEncPassword());
                    }
                    restServer.start();
                }

                // if REST-only, skip remaining logic (REST server thread will keep the VM running)
                if (cliConfig.isRestOnly()) return;

                try {
                    // determine sync config
                    SyncConfig syncConfig;
                    if (cliConfig.getXmlConfig() != null) {
                        syncConfig = loadXmlFile(new File(cliConfig.getXmlConfig()));
                    } else {
                        syncConfig = CliHelper.parseSyncConfig(cliConfig, args);
                    }

                    // create the sync instance
                    final EcsSync sync = new EcsSync();
                    sync.setSyncConfig(syncConfig);

                    // register for REST access
                    SyncJobService.getInstance().registerJob(sync);

                    // start sync job (this blocks until the sync is complete)
                    sync.run();

                    // print completion stats
                    System.out.print(sync.getStats().getStatsString());
                    if (sync.getStats().getObjectsFailed() > 0) exitCode = 3;
                } finally {
                    if (restServer != null) try {
                        restServer.stop(0);
                    } catch (Throwable t) {
                        log.warn("could not stop REST service", t);
                    }
                }
            }
        } catch (ConfigurationException e) {
            System.err.println(e.getMessage());
            System.out.println("    use --help for a detailed (quite long) list of options");
            exitCode = 1;
        } catch (Throwable t) {
            t.printStackTrace();
            exitCode = 2;
        }

        // 0 = completed with no failures, 1 = invalid options, 2 = unexpected error, 3 = completed with some object failures
        System.exit(exitCode);
    }

    private static SyncConfig loadXmlFile(File xmlFile) throws JAXBException {
        List<Class<?>> pluginClasses = new ArrayList<>();
        pluginClasses.add(SyncConfig.class);
        for (ConfigWrapper<?> wrapper : ConfigUtil.allStorageConfigWrappers()) {
            pluginClasses.add(wrapper.getTargetClass());
        }
        for (ConfigWrapper<?> wrapper : ConfigUtil.allFilterConfigWrappers()) {
            pluginClasses.add(wrapper.getTargetClass());
        }
        return (SyncConfig) JAXBContext.newInstance(pluginClasses.toArray(new Class[0]))
                .createUnmarshaller().unmarshal(xmlFile);
    }

    private static String versionLine() {
        return EcsSync.class.getSimpleName() + (VERSION == null ? "" : " v" + VERSION);
    }
}
