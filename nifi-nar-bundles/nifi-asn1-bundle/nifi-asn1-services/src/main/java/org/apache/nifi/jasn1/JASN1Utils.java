package org.apache.nifi.jasn1;

import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;

class JASN1Utils {

    static Class getSeqOfElementType(Field seqOfField) {
        final ParameterizedType seqOfGen = (ParameterizedType) seqOfField.getGenericType();
        return (Class) seqOfGen.getActualTypeArguments()[0];
    }
}
