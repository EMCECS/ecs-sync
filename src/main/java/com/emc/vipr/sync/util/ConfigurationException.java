package com.emc.vipr.sync.util;

public class ConfigurationException extends RuntimeException {
    public ConfigurationException() {
    }

    public ConfigurationException(String s) {
        super(s);
    }

    public ConfigurationException(String s, Throwable throwable) {
        super(s, throwable);
    }

    public ConfigurationException(Throwable throwable) {
        super(throwable);
    }
}
