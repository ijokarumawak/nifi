package org.apache.nifi.jasn1;

import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.util.MockComponentLog;
import org.junit.Test;

import java.io.InputStream;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class TestJASN1RecordReader {

    @Test
    public void testBasicTypes() throws Exception {


        try (final InputStream input = TestJASN1RecordReader.class.getResourceAsStream("/examples/basic-types.dat")) {

            final JASN1RecordReader reader = new JASN1RecordReader("org.apache.nifi.jasn1.example.BasicTypes",
                new RecordSchemaProvider(), Thread.currentThread().getContextClassLoader(),
                input, new MockComponentLog("id", new JASN1Reader()));

            final RecordSchema schema = reader.getSchema();
            assertEquals("BasicTypes", schema.getSchemaName().orElse(null));

            Record record = reader.nextRecord(true, false);
            assertNotNull(record);

            assertEquals(true, record.getAsBoolean("b"));
            assertEquals(789, record.getAsInt("i").intValue());
            assertArrayEquals(new byte[]{1, 2, 3, 4, 5}, (byte[]) record.getValue("octStr"));

            record = reader.nextRecord(true, false);
            assertNull(record);
        }

    }

}
