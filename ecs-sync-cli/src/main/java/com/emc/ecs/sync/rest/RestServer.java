/*
 * Copyright (c) 2015-2021 Dell Inc. or its subsidiaries. All Rights Reserved.
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
package com.emc.ecs.sync.rest;

import com.sun.jersey.api.container.ContainerFactory;
import com.sun.jersey.api.core.PackagesResourceConfig;
import com.sun.jersey.api.core.ResourceConfig;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.core.UriBuilder;
import java.net.BindException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.concurrent.Executors;

public class RestServer {
    private static final Logger log = LoggerFactory.getLogger(RestServer.class);

    public static final String DEFAULT_HOST_NAME = "localhost";
    public static final int DEFAULT_PORT = 9200;
    public static final int MAX_BIND_ATTEMPTS = 5;

    private String hostName;
    private int port;
    private HttpServer httpServer;
    private boolean autoPortEnabled = false;

    public RestServer() {
        this(DEFAULT_HOST_NAME, DEFAULT_PORT);
    }

    public RestServer(String hostName, int port) {
        this.hostName = hostName;
        this.port = port > 0 ? port : DEFAULT_PORT;
    }

    public void start() {
        log.info("starting REST server...");
        URI serverUri;
        int bindAttempts = 0;

        try {
            do {
                serverUri = createUri();
                log.debug("Binding REST server to {}", serverUri);
                InetSocketAddress socket = new InetSocketAddress(serverUri.getPort());
                if (serverUri.getHost() != null && serverUri.getHost().length() > 0)
                    socket = new InetSocketAddress(serverUri.getHost(), serverUri.getPort());

                try {
                    bindAttempts++;
                    httpServer = HttpServer.create(socket, 0);
                } catch (BindException e) {
                    log.info("Could not bind to {}: {}", serverUri, e);
                    if (!autoPortEnabled) throw e;
                    if (bindAttempts >= MAX_BIND_ATTEMPTS)
                        throw new RuntimeException("Exceeded maximum bind attempts", e);
                    port++;
                }
            } while (httpServer == null);

            httpServer.setExecutor(Executors.newCachedThreadPool());
            httpServer.createContext(serverUri.getPath(),
                    ContainerFactory.createContainer(HttpHandler.class, createResourceConfig()));
            httpServer.start();

            log.warn("REST server listening at {}", serverUri);
        } catch (Throwable t) {
            log.error("REST server failed to start: {}", t.toString());
            throw new RuntimeException("REST server failed to start", t);
        }
    }

    public void stop(int maxWaitSeconds) {
        log.info("stopping REST server...");
        httpServer.stop(maxWaitSeconds);
    }

    protected URI createUri() {
        return UriBuilder.fromUri("http://" + hostName + "/").port(port).build();
    }

    protected ResourceConfig createResourceConfig() {
        return new PackagesResourceConfig(RestServer.class.getPackage().getName());
    }

    public String getHostName() {
        return hostName;
    }

    public int getPort() {
        return port;
    }

    public boolean isAutoPortEnabled() {
        return autoPortEnabled;
    }

    public void setAutoPortEnabled(boolean autoPortEnabled) {
        this.autoPortEnabled = autoPortEnabled;
    }
}
