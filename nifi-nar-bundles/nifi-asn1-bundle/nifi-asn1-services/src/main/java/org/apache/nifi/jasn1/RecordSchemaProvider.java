package org.apache.nifi.jasn1;

import com.beanit.jasn1.ber.types.BerBoolean;
import com.beanit.jasn1.ber.types.BerInteger;
import com.beanit.jasn1.ber.types.BerOctetString;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.SchemaIdentifier;
import org.apache.nifi.serialization.record.StandardSchemaIdentifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class RecordSchemaProvider {

    private static final Logger LOG = LoggerFactory.getLogger(RecordSchemaProvider.class);

    private final LoadingCache<Class, RecordSchema> schemaCache = Caffeine.newBuilder()
        .maximumSize(100)
        .build(this::generateRecordSchema);

    public RecordSchema get(Class type) {
        return schemaCache.get(type);
    }

    private RecordSchema generateRecordSchema(Class type) {
        final SimpleRecordSchema schema = createBlankRecordSchema(type);

        final List<RecordField> fields = Arrays.stream(type.getDeclaredFields())
            .map(this::toRecordField)
            .filter(Predicate.not(Objects::isNull))
            .collect(Collectors.toList());

        schema.setFields(fields);
        return schema;
    }

    private SimpleRecordSchema createBlankRecordSchema(Class type) {
        final SchemaIdentifier schemaId = new StandardSchemaIdentifier.Builder()
            .name(type.getCanonicalName())
            .build();
        final SimpleRecordSchema schema = new SimpleRecordSchema(schemaId);
        schema.setSchemaNamespace(type.getPackageName());
        schema.setSchemaName(type.getSimpleName());
        return schema;
    }

    private RecordField toRecordField(Field field) {
        if ((field.getModifiers() & Modifier.STATIC) == Modifier.STATIC) {
            // Filter out any static fields.
            return null;
        }

        if ("code".equals(field.getName())) {
            return null;
        }

        final Class<?> type = field.getType();

        final DataType fieldType = getDataType(type);

        return new RecordField(field.getName(), fieldType, true);
    }

    private DataType getDataType(Class<?> type) {
        // TODO: implement other mappings
        if (BerBoolean.class.isAssignableFrom(type)) {
            return RecordFieldType.BOOLEAN.getDataType();

        } else if (BerInteger.class.isAssignableFrom(type)) {
            return RecordFieldType.INT.getDataType();

        } else if (BerOctetString.class.isAssignableFrom(type)) {
            return RecordFieldType.ARRAY.getArrayDataType(RecordFieldType.BYTE.getDataType());

        } else {
            if (type.getEnclosingClass() != null) {
                // jASN1 generates an inner class having a List field named 'seqOf' to denote 'SEQ OF X'.
                try {
                    final Field seqOfField = type.getDeclaredField("seqOf");
                    if (seqOfField.getType().isAssignableFrom(List.class)) {
                        final ParameterizedType seqOfGen = (ParameterizedType) seqOfField.getGenericType();
                        final Class seqOf = (Class) seqOfGen.getActualTypeArguments()[0];
                        return RecordFieldType.ARRAY.getArrayDataType(getDataType(seqOf));
                    }
                } catch (NoSuchFieldException e) {
                    LOG.trace("{} has enclosing class {}, but doesn't have seqOf field", type, type.getEnclosingClass());
                }
            }
        }

        // Lazily define the referenced type
        return RecordFieldType.RECORD.getRecordDataType(() -> schemaCache.get(type));
    }

}
