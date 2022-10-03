package com.emc.ecs.sync.storage;

import com.emc.ecs.sync.util.RandomInputStream;
import com.emc.rest.util.StreamUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;

public class ErrorThrowingInputStreamTest {
    @Test
    public void testErrorThrowingStream() throws IOException {
        String exceptionMessage = "ErrorThrowingInputStreamTest";
        IOException exception = new IOException(exceptionMessage);
        long size = 1_000_000, byteToThrowError = 300_123;
        StreamErrorThrowingFilter.StreamErrorThrowingConfig config = new StreamErrorThrowingFilter.StreamErrorThrowingConfig(exception, byteToThrowError);

        // this should not throw an exception
        InputStream stream = new StreamErrorThrowingFilter.ErrorThrowingInputStream(new RandomInputStream(size), config);
        StreamUtil.copy(stream, new ByteArrayOutputStream(), byteToThrowError);

        // but this should
        try {
            stream = new StreamErrorThrowingFilter.ErrorThrowingInputStream(new RandomInputStream(size), config);
            StreamUtil.copy(stream, new ByteArrayOutputStream(), byteToThrowError + 1);
            Assertions.fail(String.format("reading more then %s bytes should throw an exception", byteToThrowError));
        } catch (IOException e) {
            Assertions.assertEquals(exceptionMessage, e.getMessage());
        }
    }
}
