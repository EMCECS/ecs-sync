package com.emc.ecs.sync.storage;

import com.emc.ecs.sync.EcsSync;
import com.emc.ecs.sync.config.SyncConfig;
import com.emc.ecs.sync.config.SyncOptions;
import com.emc.ecs.sync.config.storage.EcsS3Config;
import com.emc.ecs.sync.test.TestConfig;
import com.emc.ecs.sync.util.EnhancedThreadPoolExecutor;
import com.emc.ecs.sync.util.RandomInputStream;
import com.emc.object.Protocol;
import com.emc.object.s3.S3Client;
import com.emc.object.s3.S3Config;
import com.emc.object.s3.S3Exception;
import com.emc.object.s3.bean.CanonicalUser;
import com.emc.object.s3.bean.ListObjectsResult;
import com.emc.object.s3.bean.S3Object;
import com.emc.object.s3.jersey.S3JerseyClient;
import com.emc.object.s3.request.CreateBucketRequest;
import com.emc.rest.smart.SizeOverrideWriter;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientHandlerException;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.container.ContainerFactory;
import com.sun.jersey.api.core.DefaultResourceConfig;
import com.sun.jersey.api.core.ResourceConfig;
import com.sun.jersey.client.apache4.ApacheHttpClient4;
import com.sun.jersey.spi.resource.Singleton;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import org.apache.http.ConnectionClosedException;
import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.annotation.Annotation;
import java.lang.reflect.Type;
import java.net.InetSocketAddress;
import java.net.URI;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Date;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.logging.LogManager;

// This class is intended to test for truncated objects in a target compliant bucket, due to read timeouts in the source
// NOTE: was not able to reproduce any truncated objects in any of these tests - it seems the Content-Length is
//       *always* set by the ECS S3 plugin (at least since v3.3.0) during write, and therefore ECS will always reject a
//       truncated request that does not have the proper byte length with a 400 response
public class EcsS3ShortWriteTest {
    private static final Logger log = LoggerFactory.getLogger(EcsS3Test.class);

    public static final int MOCK_OBJ_SIZE = 10 * 1024 * 1024; // 10MB
    public static final int MOCK_TRUNC_SIZE = MOCK_OBJ_SIZE - 1024 * 1024; // 1MB less than the correct size
    public static final String BUCKET_NAME = "ecs-sync-truncated-write-test";
    public static final int OBJECT_RETENTION_PERIOD = 15; // 15 seconds

    private EcsS3Config ecsS3Config;
    private S3Client s3;

    @BeforeEach
    public void setup() throws Exception {
        Properties syncProperties = TestConfig.getProperties();
        String endpoint = syncProperties.getProperty(TestConfig.PROP_S3_ENDPOINT);
        final String accessKey = syncProperties.getProperty(TestConfig.PROP_S3_ACCESS_KEY_ID);
        final String secretKey = syncProperties.getProperty(TestConfig.PROP_S3_SECRET_KEY);
        final boolean useVHost = Boolean.parseBoolean(syncProperties.getProperty(TestConfig.PROP_S3_VHOST));
        Assumptions.assumeTrue(endpoint != null && accessKey != null && secretKey != null);
        final URI endpointUri = new URI(endpoint);

        S3Config s3Config;
        if (useVHost) s3Config = new S3Config(endpointUri);
        else s3Config = new S3Config(Protocol.valueOf(endpointUri.getScheme().toUpperCase()), endpointUri.getHost());
        s3Config.withPort(endpointUri.getPort()).withUseVHost(useVHost).withIdentity(accessKey).withSecretKey(secretKey);
        // to make this easier to troubleshoot, disable smart-client (cleans up the logs and we know which server everything goes to)
        s3Config.setSmartClient(false);

        s3 = new S3JerseyClient(s3Config);

        try {
            // create bucket with retention period and D@RE enabled
            s3.createBucket(new CreateBucketRequest(BUCKET_NAME)
                    .withRetentionPeriod(OBJECT_RETENTION_PERIOD)
                    .withEncryptionEnabled(true));
        } catch (S3Exception e) {
            if (!e.getErrorCode().equals("BucketAlreadyExists")) throw e;
        }

        ecsS3Config = new EcsS3Config();
        ecsS3Config.setProtocol(com.emc.ecs.sync.config.Protocol.valueOf(endpointUri.getScheme().toLowerCase()));
        ecsS3Config.setHost(endpointUri.getHost());
        ecsS3Config.setPort(endpointUri.getPort());
        ecsS3Config.setEnableVHosts(useVHost);
        ecsS3Config.setSmartClientEnabled(false);
        ecsS3Config.setAccessKey(accessKey);
        ecsS3Config.setSecretKey(secretKey);
        ecsS3Config.setBucketName(BUCKET_NAME);

        // setup header logging for HttpURLConnection (JUL)
        final InputStream inputStream = EcsS3Test.class.getResourceAsStream("/logging.properties");
        LogManager.getLogManager().readConfiguration(inputStream);
    }

    @AfterEach
    public void teardown() {
        try {
            Thread.sleep(OBJECT_RETENTION_PERIOD * 1000); // wait for retention to expire
        } catch (InterruptedException ignored) {
        }
        deleteBucket(s3, BUCKET_NAME);
        LogManager.getLogManager().reset();
    }

    public static void deleteBucket(final S3Client s3, final String bucket) {
        try {
            EnhancedThreadPoolExecutor executor = new EnhancedThreadPoolExecutor(30,
                    new LinkedBlockingDeque<>(2100), "object-deleter");

            ListObjectsResult listing = null;
            do {
                if (listing == null) listing = s3.listObjects(bucket);
                else listing = s3.listMoreObjects(listing);

                for (final S3Object summary : listing.getObjects()) {
                    executor.blockingSubmit(() -> s3.deleteObject(bucket, summary.getKey()));
                }
            } while (listing.isTruncated());

            executor.shutdown();
            executor.awaitTermination(2, TimeUnit.MINUTES);

            s3.deleteBucket(bucket);
        } catch (RuntimeException e) {
            log.error("could not delete bucket " + bucket, e);
        } catch (InterruptedException e) {
            log.error("timed out while waiting for objects to be deleted");
        }
    }

    @Test
    public void testMockServer() throws IOException {
        int delay = 2; // 2 second delay on GETs
        HttpServer mockServer = createMockServer(delay);
        mockServer.start();
        URI mockEndpoint = URI.create("http://" + mockServer.getAddress().getHostName() + ":" + mockServer.getAddress().getPort());
        try {
            Client client = ApacheHttpClient4.create();

            // test chunked
            long start = System.currentTimeMillis();
            ClientResponse response = client.resource(mockEndpoint).path("/" + BUCKET_NAME).path("/chunked").get(ClientResponse.class);
            // make sure response is delayed
            Assertions.assertTrue(System.currentTimeMillis() - start > delay);
            // assert the whole content-length was returned
            Assertions.assertEquals(MOCK_OBJ_SIZE, response.getLength());
            // and chunked-encoding *was* used
            Assertions.assertEquals("chunked", response.getHeaders().getFirst("Transfer-encoding"));
            // but only half the data was sent
            Assertions.assertEquals(MOCK_TRUNC_SIZE, response.getEntity(byte[].class).length);

            // test non-chunked
            start = System.currentTimeMillis();
            response = client.resource(mockEndpoint).path("/" + BUCKET_NAME).path("/non-chunked").get(ClientResponse.class);
            // make sure response is delayed
            Assertions.assertTrue(System.currentTimeMillis() - start > delay);
            // assert the whole content-length was returned
            Assertions.assertEquals(MOCK_OBJ_SIZE, response.getLength());
            // and chunked-encoding was *not* used
            Assertions.assertNull(response.getHeaders().getFirst("Transfer-encoding"));
            try {
                // Apache will throw an exception when we try to actually read the entity because it doesn't match the content-length
                response.getEntity(byte[].class);
                Assertions.fail("Apache httpclient should have thrown exception in non-chunked read");
            } catch (ClientHandlerException e) {
                Assertions.assertTrue(e.getCause() instanceof ConnectionClosedException);
                Assertions.assertTrue(e.getCause().getMessage().contains("Premature end of Content-Length"));
            }

            // chunked with HttpURLConnection
            client = Client.create();
            response = client.resource(mockEndpoint).path("/" + BUCKET_NAME).path("/chunked").get(ClientResponse.class);
            Assertions.assertEquals(MOCK_OBJ_SIZE, response.getLength());
            Assertions.assertEquals("chunked", response.getHeaders().getFirst("Transfer-encoding"));
            Assertions.assertEquals(MOCK_TRUNC_SIZE, response.getEntity(byte[].class).length);

            // non-chunked with HttpURLConnection
            response = client.resource(mockEndpoint).path("/" + BUCKET_NAME).path("/non-chunked").get(ClientResponse.class);
            Assertions.assertEquals(MOCK_OBJ_SIZE, response.getLength());
            Assertions.assertNull(response.getHeaders().getFirst("Transfer-encoding"));
            Assertions.assertEquals(MOCK_TRUNC_SIZE, response.getEntity(byte[].class).length);

            // test list-bucket with s3 client
            ListObjectsResult result = new S3JerseyClient(new S3Config(mockEndpoint)).listObjects(BUCKET_NAME);
            Assertions.assertEquals(2, result.getObjects().size());
            S3Object chunkedObject = result.getObjects().get(0);
            Assertions.assertEquals("chunked", chunkedObject.getKey());
            Assertions.assertNotNull(chunkedObject.getLastModified());
            Assertions.assertEquals(new Long(MOCK_OBJ_SIZE), chunkedObject.getSize());
            Assertions.assertNotNull(chunkedObject.getETag());
            Assertions.assertEquals("joe", chunkedObject.getOwner().getId());
            S3Object nonChunkedObject = result.getObjects().get(1);
            Assertions.assertEquals("non-chunked", nonChunkedObject.getKey());
            Assertions.assertNotNull(nonChunkedObject.getLastModified());
            Assertions.assertEquals(new Long(MOCK_OBJ_SIZE), nonChunkedObject.getSize());
            Assertions.assertNotNull(nonChunkedObject.getETag());
            Assertions.assertEquals("joe", nonChunkedObject.getOwner().getId());
        } finally {
            mockServer.stop(10);
        }
    }

    @Test
    public void testTruncatedReadErrors() throws Exception {
        HttpServer mockServer = createMockServer(0);
        mockServer.start();
        URI mockEndpoint = URI.create("http://" + mockServer.getAddress().getHostName() + ":" + mockServer.getAddress().getPort());
        try {
            EcsS3Config source = new EcsS3Config();
            source.setProtocol(com.emc.ecs.sync.config.Protocol.http);
            source.setHost(mockEndpoint.getHost());
            source.setPort(mockEndpoint.getPort());
            source.setAccessKey("foo");
            source.setSecretKey("doesnotmatter");
            source.setSmartClientEnabled(false);
            source.setBucketName(BUCKET_NAME);

            com.emc.ecs.sync.config.storage.TestConfig target = new com.emc.ecs.sync.config.storage.TestConfig();
            target.setDiscardData(false);
            target.setReadData(true);

            SyncConfig syncConfig = new SyncConfig()
                    .withOptions(new SyncOptions()
                            .withRetryAttempts(0) // disable retries
                            .withEstimationEnabled(false)) // disable estimation
                    .withSource(source)
                    .withTarget(target);

            EcsSync sync = new EcsSync();
            sync.setSyncConfig(syncConfig);
            sync.run();

            Assertions.assertEquals(0, sync.getStats().getObjectsComplete());
            Assertions.assertEquals(2, sync.getStats().getObjectsFailed());
        } finally {
            mockServer.stop(10);
        }
    }

    @Test
    public void testTruncatedWriteApache() throws IOException {
        HttpServer mockServer = createMockServer(0);
        mockServer.start();
        URI mockEndpoint = URI.create("http://" + mockServer.getAddress().getHostName() + ":" + mockServer.getAddress().getPort());
        try {
            EcsS3Config source = new EcsS3Config();
            source.setProtocol(com.emc.ecs.sync.config.Protocol.http);
            source.setHost(mockEndpoint.getHost());
            source.setPort(mockEndpoint.getPort());
            source.setAccessKey("foo");
            source.setSecretKey("doesnotmatter");
            source.setSmartClientEnabled(false);
            source.setBucketName(BUCKET_NAME);
            source.setApacheClientEnabled(true);
            // if we don't set a timeout, this will wait forever
            source.setSocketReadTimeoutMs(5000);

            EcsS3Config target = ecsS3Config;
            target.setApacheClientEnabled(true);
            target.setSocketReadTimeoutMs(5000);

            SyncConfig syncConfig = new SyncConfig()
                    .withOptions(new SyncOptions()
                            .withRetryAttempts(0) // disable retries
                            .withEstimationEnabled(false)) // disable estimation
                    .withSource(source)
                    .withTarget(target);

            EcsSync sync = new EcsSync();
            sync.setSyncConfig(syncConfig);
            sync.run();

            Assertions.assertEquals(0, sync.getStats().getObjectsComplete());
            Assertions.assertEquals(2, sync.getStats().getObjectsFailed());

            // verify target bucket is still empty (no objects should have been committed)
            Assertions.assertEquals(0, s3.listObjects(BUCKET_NAME).getObjects().size());
        } finally {
            mockServer.stop(10);
        }
    }

    @Test
    public void testTruncatedWriteURLConnection() throws IOException {
        HttpServer mockServer = createMockServer(0);
        mockServer.start();
        URI mockEndpoint = URI.create("http://" + mockServer.getAddress().getHostName() + ":" + mockServer.getAddress().getPort());
        try {
            EcsS3Config source = new EcsS3Config();
            source.setProtocol(com.emc.ecs.sync.config.Protocol.http);
            source.setHost(mockEndpoint.getHost());
            source.setPort(mockEndpoint.getPort());
            source.setAccessKey("foo");
            source.setSecretKey("doesnotmatter");
            source.setSmartClientEnabled(false);
            source.setBucketName(BUCKET_NAME);
            source.setApacheClientEnabled(false);
            // if we don't set a timeout, this will wait forever
            source.setSocketReadTimeoutMs(5000);

            EcsS3Config target = ecsS3Config;
            target.setApacheClientEnabled(false);
            target.setSocketReadTimeoutMs(5000);

            SyncConfig syncConfig = new SyncConfig()
                    .withOptions(new SyncOptions()
                            .withRetryAttempts(0) // disable retries
                            .withEstimationEnabled(false)) // disable estimation
                    .withSource(source)
                    .withTarget(target);

            EcsSync sync = new EcsSync();
            sync.setSyncConfig(syncConfig);
            sync.run();

            Assertions.assertEquals(0, sync.getStats().getObjectsComplete());
            Assertions.assertEquals(2, sync.getStats().getObjectsFailed());

            // verify target bucket is still empty (no objects should have been committed)
            Assertions.assertEquals(0, s3.listObjects(BUCKET_NAME).getObjects().size());
        } finally {
            mockServer.stop(10);
        }
    }

    @Test
    public void testTimeoutURLConnection() throws IOException {
        testTimeout(false);
    }

    @Test
    public void testTimeoutApacheClient() throws IOException {
        testTimeout(true);
    }

    private void testTimeout(boolean useApacheClient) throws IOException {
        HttpServer mockServer = createMockServer(11);
        mockServer.start();
        URI mockEndpoint = URI.create("http://" + mockServer.getAddress().getHostName() + ":" + mockServer.getAddress().getPort());
        try {
            EcsS3Config source = new EcsS3Config();
            source.setProtocol(com.emc.ecs.sync.config.Protocol.http);
            source.setHost(mockEndpoint.getHost());
            source.setPort(mockEndpoint.getPort());
            source.setAccessKey("foo");
            source.setSecretKey("doesnotmatter");
            source.setSmartClientEnabled(false);
            source.setBucketName(BUCKET_NAME);
            source.setApacheClientEnabled(useApacheClient);
            source.setSocketReadTimeoutMs(5000);

            EcsS3Config target = ecsS3Config;
            target.setApacheClientEnabled(useApacheClient);
            target.setSocketReadTimeoutMs(5000);

            SyncConfig syncConfig = new SyncConfig()
                    .withOptions(new SyncOptions()
                            .withRetryAttempts(0) // disable retries
                            .withEstimationEnabled(false)) // disable estimation
                    .withSource(source)
                    .withTarget(target);

            EcsSync sync = new EcsSync();
            sync.setSyncConfig(syncConfig);
            sync.run();

            Assertions.assertEquals(0, sync.getStats().getObjectsComplete());
            Assertions.assertEquals(2, sync.getStats().getObjectsFailed());

            // TODO: sometimes the object is created, but does not show in a list right away - figure out why
            try {
                Thread.sleep(2000);
            } catch (InterruptedException ignored) {
            }

            // verify target bucket is still empty (no objects should have been committed)
            Assertions.assertEquals(0, s3.listObjects(BUCKET_NAME).getObjects().size());
        } finally {
            mockServer.stop(10);
        }
    }

    HttpServer createMockServer(int requestDelayInSeconds) throws IOException {
        // create an HTTP server
        InetSocketAddress socket = new InetSocketAddress("localhost", 0); // let the system pick a port
        HttpServer httpServer = HttpServer.create(socket, 0);

        // configure content-length override in Jersey
        ResourceConfig resourceConfig = new DefaultResourceConfig();
        resourceConfig.getSingletons().add(new MockServerResource());
        resourceConfig.getSingletons().add(new DelayedByteArrayWriter(requestDelayInSeconds));
        resourceConfig.getSingletons().add(new DelayedInputStreamWriter(requestDelayInSeconds));

        // assign our mock server Jersey resource to the root context
        httpServer.createContext("/", ContainerFactory.createContainer(HttpHandler.class, resourceConfig));
        httpServer.setExecutor(Executors.newCachedThreadPool());

        return httpServer;
    }

    @Singleton
    @Path("/" + BUCKET_NAME)
    public static class MockServerResource {
        String eTag = "\"acbd18db4cc2f85cedef654fccc4a4d8\"";
        final byte[] buffer;
        Random random = new Random();

        public MockServerResource() {
            buffer = new byte[MOCK_TRUNC_SIZE];
        }

        @GET
        @Produces("application/xml")
        public ListObjectsResult list() {
            Date yesterday = Date.from(Instant.now().minus(1, ChronoUnit.DAYS));

            S3Object chunkedObject = new S3Object();
            chunkedObject.setKey("chunked");
            chunkedObject.setLastModified(yesterday);
            chunkedObject.setSize((long) MOCK_OBJ_SIZE);
            chunkedObject.setETag(eTag);
            chunkedObject.setOwner(new CanonicalUser("joe", "joe"));

            S3Object nonChunkedObject = new S3Object();
            nonChunkedObject.setKey("non-chunked");
            nonChunkedObject.setLastModified(yesterday);
            nonChunkedObject.setSize((long) MOCK_OBJ_SIZE);
            nonChunkedObject.setETag(eTag);
            nonChunkedObject.setOwner(new CanonicalUser("joe", "joe"));

            ListObjectsResult result = new ListObjectsResult();
            result.setBucketName(BUCKET_NAME);
            result.setMaxKeys(1000);
            result.setTruncated(false);
            result.setObjects(Arrays.asList(chunkedObject, nonChunkedObject));

            return result;
        }

        @HEAD
        @Path("chunked")
        public Response chunkedHead() {
            return Response.ok()
                    .header("Content-Length", MOCK_OBJ_SIZE)
                    .header("ETag", eTag)
                    .build();
        }

        @GET
        @Path("chunked")
        public Response chunked() {
            return Response.ok(new RandomInputStream(buffer.length))
                    .header("Content-Length", MOCK_OBJ_SIZE)
                    .header("ETag", eTag)
                    .build();
        }

        @HEAD
        @Path("non-chunked")
        public Response nonChunkedHead() {
            return Response.ok()
                    .header("Content-Length", MOCK_OBJ_SIZE)
                    .header("ETag", eTag)
                    .build();
        }

        @GET
        @Path("non-chunked")
        public Response nonChunked() {
            random.nextBytes(buffer);
            SizeOverrideWriter.setEntitySize((long) MOCK_OBJ_SIZE);
            return Response.ok(buffer)
                    .header("ETag", eTag)
                    .build();
        }
    }

    @Produces({"application/octet-stream", "*/*"})
    class DelayedByteArrayWriter extends SizeOverrideWriter.ByteArray {
        private final int requestDelayInSeconds;

        public DelayedByteArrayWriter(int requestDelayInSeconds) {
            this.requestDelayInSeconds = requestDelayInSeconds;
        }

        @Override
        public void writeTo(byte[] bytes, Class<?> type, Type genericType, Annotation[] annotations, MediaType mediaType, MultivaluedMap<String, Object> httpHeaders, OutputStream entityStream) throws IOException, WebApplicationException {
            super.writeTo(bytes, type, genericType, annotations, mediaType, httpHeaders, entityStream);
            delay(requestDelayInSeconds);
        }
    }

    @Produces({"application/octet-stream", "*/*"})
    class DelayedInputStreamWriter extends SizeOverrideWriter.InputStream {
        private final int requestDelayInSeconds;

        public DelayedInputStreamWriter(int requestDelayInSeconds) {
            this.requestDelayInSeconds = requestDelayInSeconds;
        }

        @Override
        public void writeTo(java.io.InputStream inputStream, Class<?> type, Type genericType, Annotation[] annotations, MediaType mediaType, MultivaluedMap<String, Object> httpHeaders, OutputStream entityStream) throws IOException, WebApplicationException {
            super.writeTo(inputStream, type, genericType, annotations, mediaType, httpHeaders, entityStream);
            delay(requestDelayInSeconds);
        }
    }

    void delay(int seconds) {
        try {
            if (seconds > 0) Thread.sleep(seconds * 1000L);
        } catch (InterruptedException e) {
            log.warn("interrupted during delay");
        }
    }

}
