package com.emc.ecs.sync.service;

public class JobNotFoundException extends RuntimeException {
    public JobNotFoundException() {
    }

    public JobNotFoundException(String message) {
        super(message);
    }

    public JobNotFoundException(String message, Throwable cause) {
        super(message, cause);
    }

    public JobNotFoundException(Throwable cause) {
        super(cause);
    }

    public JobNotFoundException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
