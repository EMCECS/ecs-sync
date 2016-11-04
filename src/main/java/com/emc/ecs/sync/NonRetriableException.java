package com.emc.ecs.sync;

public class NonRetriableException extends RuntimeException {
    public NonRetriableException() {
    }

    public NonRetriableException(String message) {
        super(message);
    }

    public NonRetriableException(String message, Throwable cause) {
        super(message, cause);
    }

    public NonRetriableException(Throwable cause) {
        super(cause);
    }

    public NonRetriableException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
