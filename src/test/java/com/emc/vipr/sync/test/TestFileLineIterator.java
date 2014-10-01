package com.emc.vipr.sync.test;

import com.emc.vipr.sync.util.FileLineIterator;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;

public class TestFileLineIterator {
    @Test
    public void testComments() throws IOException {
        File file = writeTempFile("alpha=bravo\n" + // standard
                "charlie=delta# comment here\n" + // regular comment
                "   # line should be skipped\n" + // comment line
                "" + // empty line
                "echo\\#one=foxtrot\n" + // escaped hash
                "  golf=hotel \t\n"); // trim test

        FileLineIterator i = new FileLineIterator(file.getPath());
        String line = i.next();
        Assert.assertEquals("alpha=bravo", line);
        line = i.next();
        Assert.assertEquals("charlie=delta", line);
        line = i.next();
        Assert.assertEquals("echo#one=foxtrot", line);
        line = i.next();
        Assert.assertEquals("golf=hotel", line);
    }

    public File writeTempFile(String content) throws IOException {
        File tempFile = File.createTempFile("test", null);
        tempFile.deleteOnExit();

        OutputStream out = new FileOutputStream(tempFile);
        out.write(content.getBytes("UTF-8"));
        out.close();

        return tempFile;
    }
}
