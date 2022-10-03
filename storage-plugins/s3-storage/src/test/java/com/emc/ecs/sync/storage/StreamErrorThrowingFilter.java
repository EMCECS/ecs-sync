package com.emc.ecs.sync.storage;

import com.emc.ecs.sync.config.annotation.FilterConfig;
import com.emc.ecs.sync.filter.AbstractFilter;
import com.emc.ecs.sync.filter.InternalFilter;
import com.emc.ecs.sync.model.ObjectContext;
import com.emc.ecs.sync.model.SyncObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.atomic.AtomicLong;

public class StreamErrorThrowingFilter extends AbstractFilter<StreamErrorThrowingFilter.StreamErrorThrowingConfig> {
    private static final Logger log = LoggerFactory.getLogger(StreamErrorThrowingFilter.class);

    @Override
    public void filter(ObjectContext objectContext) {
        InputStream dataStream = objectContext.getObject().getDataStream();

        // wrap the object's data stream with one that will throw an exception at the configured byte
        objectContext.getObject().setDataStream(new ErrorThrowingInputStream(dataStream, config));

        getNext().filter(objectContext);
    }

    @Override
    public SyncObject reverseFilter(ObjectContext objectContext) {
        return getNext().reverseFilter(objectContext);
    }

    @FilterConfig(cliName = "stream-error")
    @InternalFilter
    public static class StreamErrorThrowingConfig {
        private final IOException exception;
        private long throwOnThisByte;

        public StreamErrorThrowingConfig(IOException exception, long throwOnThisByte) {
            this.exception = exception;
            this.throwOnThisByte = throwOnThisByte;
        }

        public IOException getException() {
            return exception;
        }

        public long getThrowOnThisByte() {
            return throwOnThisByte;
        }

        public void setThrowOnThisByte(long throwOnThisByte) {
            this.throwOnThisByte = throwOnThisByte;
        }
    }

    public static class ErrorThrowingInputStream extends FilterInputStream {
        private final StreamErrorThrowingConfig config;
        private final AtomicLong bytesRead = new AtomicLong();
        private final AtomicLong markPosition = new AtomicLong(-1);

        public ErrorThrowingInputStream(InputStream in, StreamErrorThrowingConfig config) {
            super(in);
            this.config = config;
        }

        @Override
        public int read() {
            throw new UnsupportedOperationException("single-byte read should never be called");
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            int read = super.read(b, off, maxBytesToRead(len));
            if (read > 0) bytesRead.addAndGet(read);
            return read;
        }

        @Override
        public synchronized void mark(int i) {
            super.mark(i);
            markPosition.set(bytesRead.get());
        }

        @Override
        public synchronized void reset() throws IOException {
            super.reset();
            bytesRead.set(markPosition.get());
        }

        private int maxBytesToRead(int len) throws IOException {
            if (bytesRead.get() >= config.getThrowOnThisByte()) {
                log.info("{} bytes read >= {} (configured byte to throw) - throwing exception",
                        bytesRead.get(), config.getThrowOnThisByte());
                throw new IOException(config.getException().getMessage(), config.getException()); // wrap so we don't lose our stack trace
            } else {
                return (int) Math.min(len, config.getThrowOnThisByte() - bytesRead.get());
            }
        }
    }
}