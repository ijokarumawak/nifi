package org.apache.nifi.jasn1;

import com.beanit.jasn1.ber.types.BerBoolean;
import com.beanit.jasn1.ber.types.BerInteger;
import com.beanit.jasn1.ber.types.BerOctetString;
import com.beanit.jasn1.ber.types.BerType;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.function.Supplier;

public class JASN1RecordReader implements RecordReader {

    private final Class<BerType> rootClass;
    private final RecordSchemaProvider schemaProvider;
    private final ClassLoader classLoader;
    private final InputStream inputStream;
    private final ComponentLog logger;

    private BerType model;

    private <T> T withClassLoader(Supplier<T> supplier) {
        final ClassLoader originalContextClassLoader = Thread.currentThread().getContextClassLoader();
        try {
            if (classLoader != null) {
                Thread.currentThread().setContextClassLoader(classLoader);
            }

            return supplier.get();
        } finally {
            if (classLoader != null && originalContextClassLoader != null) {
                Thread.currentThread().setContextClassLoader(originalContextClassLoader);
            }
        }
    }

    @SuppressWarnings("unchecked")
    public JASN1RecordReader(String rootClassName, RecordSchemaProvider schemaProvider, ClassLoader classLoader,
                             InputStream inputStream, ComponentLog logger) {

        this.schemaProvider = schemaProvider;
        this.classLoader = classLoader;
        this.inputStream = inputStream;
        this.logger = logger;

        this.rootClass = withClassLoader(() -> {
            try {
                return (Class<BerType>) classLoader.loadClass(rootClassName);
            } catch (ClassNotFoundException e) {
                throw new RuntimeException("The root class " + rootClassName + " was not found.", e);
            }
        });
    }

    @Override
    public Record nextRecord(boolean coerceTypes, boolean dropUnknownFields) throws IOException, MalformedRecordException {

        return withClassLoader(() -> {
            if (model == null) {
                try {
                    model = rootClass.getDeclaredConstructor().newInstance();
                } catch (ReflectiveOperationException e) {
                    throw new RuntimeException("Failed to instantiate " + rootClass.getCanonicalName(), e);
                }

                try {
                    final int decode = model.decode(inputStream);
                    logger.debug("Decoded {} bytes into {}", new Object[]{decode, model});
                } catch (IOException e) {
                    throw new RuntimeException("Failed to decode " + rootClass.getCanonicalName(), e);
                }

                // TODO: specify record path to point the target.
                final RecordSchema recordSchema = schemaProvider.get(rootClass);
                final MapRecord record = new MapRecord(recordSchema, new HashMap<>());

                for (RecordField field : recordSchema.getFields()) {
                    final Object value;
                    try {
                        value = rootClass.getDeclaredMethod(toGetterMethod(field.getFieldName())).invoke(model);
                    } catch (ReflectiveOperationException e) {
                        throw new RuntimeException("Failed to get value of field " + field, e);
                    }
                    logger.info("{}={}", new Object[]{field.getFieldName(), value});
                    record.setValue(field, convertBerValue(value));
                }

                return record;

            } else {
                return null;
            }

        });
    }

    private String toGetterMethod(String fieldName) {
        return "get" + fieldName.substring(0, 1).toUpperCase() + fieldName.substring(1);
    }

    private Object convertBerValue(Object value) {
        if (value instanceof BerBoolean) {
            return ((BerBoolean) value).value;

        } else if (value instanceof BerInteger) {
            return ((BerInteger) value).value;

        } else if (value instanceof BerOctetString) {
            return ((BerOctetString) value).value;

        }
        return null;
    }

    @Override
    public RecordSchema getSchema() throws MalformedRecordException {
        return withClassLoader(() -> schemaProvider.get(rootClass));
    }

    @Override
    public void close() throws IOException {

    }
}
