package com.emc.ecs.sync.util;

import org.apache.logging.log4j.LogManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;

public final class LoggingUtil {
    private static final Logger log = LoggerFactory.getLogger(LoggingUtil.class);

    public static Level getRootLogLevel() {
        org.apache.logging.log4j.Level level = LogManager.getRootLogger().getLevel();
        if (org.apache.logging.log4j.Level.TRACE.equals(level) || org.apache.logging.log4j.Level.ALL.equals(level)) {
            return Level.TRACE;
        } else if (org.apache.logging.log4j.Level.DEBUG.equals(level)) {
            return Level.DEBUG;
        } else if (org.apache.logging.log4j.Level.INFO.equals(level)) {
            return Level.INFO;
        } else if (org.apache.logging.log4j.Level.WARN.equals(level)) {
            return Level.WARN;
        }
        return Level.ERROR;
    }

    // Note: this will *only* take effect if the log implementation is log4j2
    public static void setRootLogLevel(Level logLevel) {
        // try to avoid a runtime dependency on log4j (untested)
        try {
            if (Level.TRACE == logLevel) {
                org.apache.logging.log4j.core.config.Configurator.setRootLevel(org.apache.logging.log4j.Level.TRACE);
            } else if (Level.DEBUG == logLevel) {
                org.apache.logging.log4j.core.config.Configurator.setRootLevel(org.apache.logging.log4j.Level.DEBUG);
            } else if (Level.INFO == logLevel) {
                org.apache.logging.log4j.core.config.Configurator.setRootLevel(org.apache.logging.log4j.Level.INFO);
            } else if (Level.WARN == logLevel) {
                org.apache.logging.log4j.core.config.Configurator.setRootLevel(org.apache.logging.log4j.Level.WARN);
            } else if (Level.ERROR == logLevel) {
                org.apache.logging.log4j.core.config.Configurator.setRootLevel(org.apache.logging.log4j.Level.ERROR);
            }
        } catch (Throwable t) {
            log.warn("could not configure log4j (perhaps you're using a different logger, which is fine)", t);
        }
    }

    private LoggingUtil() {
    }
}
