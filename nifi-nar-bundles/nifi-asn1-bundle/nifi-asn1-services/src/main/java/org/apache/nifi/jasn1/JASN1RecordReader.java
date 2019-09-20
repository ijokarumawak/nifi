package org.apache.nifi.jasn1;

import com.beanit.jasn1.ber.types.BerBoolean;
import com.beanit.jasn1.ber.types.BerInteger;
import com.beanit.jasn1.ber.types.BerOctetString;
import com.beanit.jasn1.ber.types.BerType;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.type.ArrayDataType;
import org.apache.nifi.util.StringUtils;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.function.Supplier;

public class JASN1RecordReader implements RecordReader {

    private final Class<? extends BerType> rootClass;
    private final String recordField;
    private final RecordSchemaProvider schemaProvider;
    private final ClassLoader classLoader;
    private final InputStream inputStream;
    private final ComponentLog logger;

    private BerType model;
    private List<BerType> recordModels;
    private Iterator<BerType> recordModelIterator;

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
    public JASN1RecordReader(String rootClassName, String recordField,
                             RecordSchemaProvider schemaProvider, ClassLoader classLoader,
                             InputStream inputStream, ComponentLog logger) {

        this.schemaProvider = schemaProvider;
        this.classLoader = classLoader;
        this.inputStream = inputStream;
        this.logger = logger;

        this.recordField = recordField;
        this.rootClass = withClassLoader(() -> {
            try {
                return (Class<? extends BerType>) classLoader.loadClass(rootClassName);
            } catch (ClassNotFoundException e) {
                throw new RuntimeException("The root class " + rootClassName + " was not found.", e);
            }
        });
    }

    @SuppressWarnings("unchecked")
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
                    logger.debug("Decoded {} bytes into {}", new Object[]{decode, model.getClass()});
                } catch (IOException e) {
                    throw new RuntimeException("Failed to decode " + rootClass.getCanonicalName(), e);
                }

                if (StringUtils.isEmpty(recordField)) {
                    recordModels = Collections.singletonList(model);
                } else {
                    try {
                        final Method recordModelGetter = model.getClass().getMethod(toGetterMethod(recordField));
                        final BerType readPointModel = (BerType) recordModelGetter.invoke(model);
                        final Field seqOfField = JASN1Utils.getSeqOfField(readPointModel.getClass());
                        if (seqOfField != null) {
                            final Class seqOf = JASN1Utils.getSeqOfElementType(seqOfField);
                            recordModels = (List<BerType>) invokeGetter(readPointModel, toGetterMethod(seqOf.getSimpleName()));
                        } else {
                            recordModels = Collections.singletonList(readPointModel);
                        }
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }

                recordModelIterator = recordModels.iterator();
            }

            if (recordModelIterator.hasNext()) {
                return convertBerRecord(recordModelIterator.next());
            } else {
                return null;
            }

        });
    }

    private String toGetterMethod(String fieldName) {
        return "get" + fieldName.substring(0, 1).toUpperCase() + fieldName.substring(1);
    }

    @SuppressWarnings("unchecked")
    private Object convertBerValue(String name, DataType dataType, BerType instance, Object value) {
        if (value instanceof BerBoolean) {
            return ((BerBoolean) value).value;

        } else if (value instanceof BerInteger) {
            final BerInteger berInteger = ((BerInteger) value);

            if (RecordFieldType.INT.equals(dataType.getFieldType())) {
                return berInteger.value.intValue();
            }

            return ((BerInteger) value).value;

        } else if (value instanceof BerOctetString) {
            return ((BerOctetString) value).value;

        } else if (value instanceof BerType) {

            if (RecordFieldType.ARRAY.equals(dataType.getFieldType())) {
                // If the field is declared with a direct SEQUENCE OF, then this value is a Parent$Children innerclass,
                // in such a case, use the parent instance to get the seqOfContainer.
                // Otherwise, the value is a separated class holding only seqOf field.
                final BerType seqOfContainer = instance.getClass().equals(value.getClass().getEnclosingClass())
                    ? (BerType) invokeGetter(instance, toGetterMethod(name))
                    : (BerType) value;
                if (seqOfContainer == null) {
                    return null;
                }

                // Use the generic type of seqOf field to determine the getter method name.
                final Field seqOfField;
                try {
                    seqOfField = seqOfContainer.getClass().getDeclaredField("seqOf");
                } catch (NoSuchFieldException e) {
                    throw new RuntimeException(seqOfContainer + " doesn't have the expected 'seqOf' field.");
                }

                final Class seqOf = JASN1Utils.getSeqOfElementType(seqOfField);
                final String getterMethod = toGetterMethod(seqOf.getSimpleName());

                final DataType elementType = ((ArrayDataType) dataType).getElementType();
                return ((List<BerType>) invokeGetter(seqOfContainer, getterMethod)).stream()
                    .map(v -> convertBerValue(name, elementType, (BerType) value, v)).toArray();

            } else {
                return convertBerRecord((BerType) value);
            }
        }

        return null;
    }

    private Record convertBerRecord(BerType model) {
        final Class<? extends BerType> modelClass = model.getClass();
        final RecordSchema recordSchema = schemaProvider.get(modelClass);
        final MapRecord record = new MapRecord(recordSchema, new HashMap<>());

        for (RecordField field : recordSchema.getFields()) {
            final Object value = invokeGetter(model, toGetterMethod(field.getFieldName()));
            record.setValue(field, convertBerValue(field.getFieldName(), field.getDataType(), model, value));
        }

        return record;
    }

    private Object invokeGetter(BerType model, String methodName) {
        final Object value;
        final String operation = "get value from " + model.getClass().getCanonicalName() + " by " + methodName;
        try {
            value = model.getClass().getDeclaredMethod(methodName).invoke(model);
        } catch (ReflectiveOperationException e) {
            throw new RuntimeException("Failed to " + operation, e);
        }
        logger.trace("{}={}", new Object[]{operation, value});
        return value;
    }

    @Override
    public RecordSchema getSchema() throws MalformedRecordException {
        return withClassLoader(() -> schemaProvider.get(rootClass));
    }

    @Override
    public void close() throws IOException {

    }
}
