package org.apache.nifi.jasn1;

import org.apache.nifi.serialization.record.RecordFieldType;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.util.Arrays;

class JASN1Utils {

    static Class getSeqOfElementType(Field seqOfField) {
        final ParameterizedType seqOfGen = (ParameterizedType) seqOfField.getGenericType();
        return (Class) seqOfGen.getActualTypeArguments()[0];
    }

    static boolean isRecordField(Field field) {
        // Filter out any static and reserved fields.
        if ((field.getModifiers() & Modifier.STATIC) == Modifier.STATIC
            || "code".equals(field.getName())) {
            return false;
        }

        return true;
    }

    static Field getSeqOfField(Class<?> type) {
        // jASN1 generates a class having a single List field named 'seqOf' to denote 'SEQ OF X'.
        final Object[] declaredFields = Arrays.stream(type.getDeclaredFields())
            .filter(JASN1Utils::isRecordField).toArray();
        if (declaredFields.length == 1 ) {
            final Field seqOfField = (Field) declaredFields[0];
            if ("seqOf".equals(seqOfField.getName())) {
                return seqOfField;
            }
        }
        return null;
    }

}
