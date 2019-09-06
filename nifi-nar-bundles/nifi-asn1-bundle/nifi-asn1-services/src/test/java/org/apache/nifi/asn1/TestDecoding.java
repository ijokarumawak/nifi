package org.apache.nifi.asn1;

import cs.r99.r4.charging.CallEventDataFile;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.reflect.ReflectDatumWriter;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

public class TestDecoding {

    private final Logger logger = LoggerFactory.getLogger(TestDecoding.class);

    @Test
    public void testDecoding() {

        final File file = new File("/Users/koji/dev/cloudera/Telkom/jasn1/mppr_01.crc01.0532314.20577954.20190702092202");
        try (final FileInputStream input = new FileInputStream(file)) {
            final CallEventDataFile entity = new CallEventDataFile();
            final int decode = entity.decode(input);
            logger.info("Decoded {} bytes", decode);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testSchemaGeneration() {
        final Schema schema = ReflectData.get().getSchema(CallEventDataFile.class);
        final Schema nullable = makeNullable(schema);
//        logger.info(schema.toString(true));
        logger.info(nullable.toString(true));
    }


    @Test
    public void testWriteEntityToAvro() {

        final File file = new File("/Users/koji/dev/cloudera/Telkom/jasn1/mppr_01.crc01.0532314.20577954.20190702092202");
        try (final FileInputStream input = new FileInputStream(file)) {
            final CallEventDataFile entity = new CallEventDataFile();
            final int decode = entity.decode(input);
            logger.info("Decoded {} bytes", decode);

            // TODO: make every field nullable.
            final Schema schema = ReflectData.get().getSchema(entity.getClass());
            logger.info(schema.toString(true));

            ReflectDatumWriter<CallEventDataFile> datumWriter = new ReflectDatumWriter<>(schema);
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

            BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(outputStream, null);
            datumWriter.write(entity, encoder);
            encoder.flush();

            DatumReader<GenericRecord> datumReader = new GenericDatumReader<>(schema);
            BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(outputStream.toByteArray(), null);

            final GenericRecord read = datumReader.read(null, decoder);
            logger.info("Avro record={}", read);

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private Schema makeNullable(Schema schema) {
        return makeNullable(schema, Schema.createRecord(schema.getName(), schema.getDoc(),
            schema.getNamespace(), schema.isError()));
    }

    private Schema makeNullable(Schema original, Schema target) {
        final List<Schema.Field> nullableFields = original.getFields().stream().map(field -> {
            final Schema fieldSchema = field.schema();
            final Schema nullableSchema = Schema.Type.RECORD.equals(fieldSchema.getType())
                ? makeNullable(fieldSchema)
                : ReflectData.makeNullable(fieldSchema);
            final Schema.Field nullableField = new Schema.Field(field.name(), nullableSchema,
                field.doc(), field.defaultVal(), field.order());
            return nullableField;
        }).collect(Collectors.toList());
        target.setFields(nullableFields);
        return target;
    }

}
