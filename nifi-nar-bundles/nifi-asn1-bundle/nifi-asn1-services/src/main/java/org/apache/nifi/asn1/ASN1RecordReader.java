package org.apache.nifi.asn1;

import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordSchema;

import java.io.IOException;

public class ASN1RecordReader implements RecordReader {
    @Override
    public Record nextRecord(boolean coerceTypes, boolean dropUnknownFields) throws IOException, MalformedRecordException {
        return null;
    }

    @Override
    public RecordSchema getSchema() throws MalformedRecordException {
        return null;
    }

    @Override
    public void close() throws IOException {

    }
}
