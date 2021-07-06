package com.emc.ecs.sync.storage;

import com.emc.ecs.sync.config.SyncOptions;
import com.emc.ecs.sync.model.ObjectSummary;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

public class AbstractStorageTest {
    String line1 = "alpha=bravo"; // standard
    String line2 = "charlie=delta# comment here"; // regular comment
    String line3 = "   # line should be skipped"; // comment line
    String line4 = "echo\\#one=foxtrot"; // escaped hash
    String line5 = "  golf=hotel \t"; // trim test
    String line6 = "\"foo,bar\""; // quoted comma
    String line7 = "\"foo#bar\" # baz"; // quoted hash and unquoted hash - note this is invalid CSV
    String line8 = "foo,bar#, baz"; // comma after hash
    String line9 = "\"foo#bar\"\" # baz\""; // properly escaped quote
    String line10 = "\"foo#bar\"\"\" # baz"; // quoted hash and unquoted hash plus an escaped quote - also invalid
    String line11 = "\"foo \"\" bar\""; // basic escaped quote
    String line12 = "\"\"\"foo bar\"\"\""; // escaped quotes at the beginning and end of the line

    @Test
    public void testObjectSummaryParsing() {
        // in theory, comments would be stripped by LineIterator before parsing, but for simplicity,
        // this will test the handling of hashes during CSV parsing
        AbstractStorage<?> storage = new TestStorage();

        ObjectSummary summary = storage.parseListLine(line1);
        Assert.assertEquals(line1, summary.getIdentifier());
        summary = storage.parseListLine(line2);
        Assert.assertEquals(line2, summary.getIdentifier());
        summary = storage.parseListLine(line3);
        Assert.assertEquals(line3, summary.getIdentifier());
        summary = storage.parseListLine(line4);
        Assert.assertEquals(line4, summary.getIdentifier());
        summary = storage.parseListLine(line5);
        Assert.assertEquals(line5, summary.getIdentifier());
        summary = storage.parseListLine(line6);
        Assert.assertEquals(unwrapQuotes(line6), summary.getIdentifier());
        try {
            storage.parseListLine(line7);
            Assert.fail("invalid CSV line 7 did not throw exception");
        } catch (RuntimeException e) {
            Assert.assertNotNull(e.getCause());
            Assert.assertTrue(e.getCause() instanceof IOException);
            Assert.assertTrue(e.getCause().getMessage().contains("invalid char"));
        }
        summary = storage.parseListLine(line8);
        Assert.assertEquals("foo", summary.getIdentifier());
        summary = storage.parseListLine(line9);
        Assert.assertEquals(unwrapQuotes(line9), summary.getIdentifier());
        try {
            storage.parseListLine(line10);
            Assert.fail("invalid CSV line 10 did not throw exception");
        } catch (RuntimeException e) {
            Assert.assertNotNull(e.getCause());
            Assert.assertTrue(e.getCause() instanceof IOException);
            Assert.assertTrue(e.getCause().getMessage().contains("invalid char"));
        }
        summary = storage.parseListLine(line11);
        Assert.assertEquals(unwrapQuotes(line11), summary.getIdentifier());
        summary = storage.parseListLine(line12);
        Assert.assertEquals(unwrapQuotes(line12), summary.getIdentifier());
    }

    @Test
    public void testObjectSummaryRawParsing() {
        AbstractStorage<?> storage = new TestStorage();
        storage.withOptions(new SyncOptions().withSourceListFileRawValues(true));

        ObjectSummary summary = storage.parseListLine(line1);
        Assert.assertEquals(line1, summary.getIdentifier());
        summary = storage.parseListLine(line2);
        Assert.assertEquals(line2, summary.getIdentifier());
        summary = storage.parseListLine(line3);
        Assert.assertEquals(line3, summary.getIdentifier());
        summary = storage.parseListLine(line4);
        Assert.assertEquals(line4, summary.getIdentifier());
        summary = storage.parseListLine(line5);
        Assert.assertEquals(line5, summary.getIdentifier());
        summary = storage.parseListLine(line6);
        Assert.assertEquals(line6, summary.getIdentifier());
        summary = storage.parseListLine(line7);
        Assert.assertEquals(line7, summary.getIdentifier());
        summary = storage.parseListLine(line8);
        Assert.assertEquals(line8, summary.getIdentifier());
        summary = storage.parseListLine(line9);
        Assert.assertEquals(line9, summary.getIdentifier());
        summary = storage.parseListLine(line10);
        Assert.assertEquals(line10, summary.getIdentifier());
        summary = storage.parseListLine(line11);
        Assert.assertEquals(line11, summary.getIdentifier());
        summary = storage.parseListLine(line12);
        Assert.assertEquals(line12, summary.getIdentifier());
    }

    String unwrapQuotes(String value) {
        return value.replaceFirst("^\"", "").replaceFirst("\"$", "")
                .replaceAll("\"\"", "\"");
    }
}
