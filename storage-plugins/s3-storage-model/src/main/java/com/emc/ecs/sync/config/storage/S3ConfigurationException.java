/*
 * Copyright (c) 2021-2022 Dell Inc. or its subsidiaries. All Rights Reserved.
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
package com.emc.ecs.sync.config.storage;

import com.emc.ecs.sync.config.ConfigurationException;

public class S3ConfigurationException extends ConfigurationException {
    public enum Error {
        ERROR_BUCKET_ACCESS_WRITE("WriteAccessDenied", "Insufficient write access on bucket"),
        ERROR_BUCKET_ACCESS_DELETE("DeleteAccessDenied", "Insufficient delete access on bucket"),
        ERROR_BUCKET_ACCESS_UNKNOWN("UnknownBucketAccess", "Unknown bucket access"),
        ERROR_INVALID_TLS_CERTIFICATE("InvalidTLSCertificate", "Invalid TLS Certificate");

        private final String errorCode;
        private final String defaultMessage;

        public String getDefaultMessage() { return defaultMessage; }

        public String getErrorCode() { return errorCode; }

        private Error(String errorCode, String errorMessage) {
            this.errorCode = errorCode;
            this.defaultMessage = errorMessage;
        }
    }

    private Error error;

    public Error getError() { return this.error; }

    public S3ConfigurationException(Error error) {
        super(error.getErrorCode(), error.getDefaultMessage());
        this.error = error;
    }

    public S3ConfigurationException(Error error, String errorMessage) {
        super(error.getErrorCode(), errorMessage);
        this.error = error;
    }

    public S3ConfigurationException(Error error, Throwable throwable) {
        super(error.getErrorCode(), error.getDefaultMessage(), throwable);
        this.error = error;
    }
}
