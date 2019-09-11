package org.apache.nifi.jasn1;

import org.apache.nifi.jasn1.example.BasicTypes;
import org.apache.nifi.jasn1.example.Composite;
import org.apache.nifi.jasn1.example.Recursive;
import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.type.ArrayDataType;
import org.apache.nifi.serialization.record.type.RecordDataType;
import org.junit.Test;

import java.util.Optional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestRecordSchemaProvider {

    @Test
    public void testBasicTypes() {
        final RecordSchemaProvider provider = new RecordSchemaProvider();
        final RecordSchema schema = provider.get(BasicTypes.class);

        assertEquals("BasicTypes", schema.getSchemaName().orElse(null));
        assertEquals("org.apache.nifi.jasn1.example", schema.getSchemaNamespace().orElse(null));

        final Optional<RecordField> bField = schema.getField("b");
        assertTrue(bField.isPresent());
        assertEquals(RecordFieldType.BOOLEAN.getDataType(), bField.get().getDataType());

        final Optional<RecordField> iField = schema.getField("i");
        assertTrue(iField.isPresent());
        assertEquals(RecordFieldType.BIGINT.getDataType(), iField.get().getDataType());

        final Optional<RecordField> octStrField = schema.getField("octStr");
        assertTrue(octStrField.isPresent());
        assertEquals(RecordFieldType.ARRAY.getArrayDataType(RecordFieldType.BYTE.getDataType()),
            octStrField.get().getDataType());
    }

    @Test
    public void testComposite() {
        final RecordSchemaProvider provider = new RecordSchemaProvider();
        final RecordSchema schema = provider.get(Composite.class);

        // Child should be a record.
        final Optional<RecordField> childField = schema.getField("child");
        assertTrue(childField.isPresent());
        final DataType childDataType = childField.get().getDataType();
        assertEquals(RecordFieldType.RECORD.getDataType(), childDataType);
        RecordSchema childSchema = ((RecordDataType) childDataType).getChildSchema();
        assertEquals("BasicTypes", childSchema.getSchemaName().orElse(null));
        assertEquals("org.apache.nifi.jasn1.example", childSchema.getSchemaNamespace().orElse(null));

        // Children should be an array of records.
        final Optional<RecordField> childrenField = schema.getField("children");
        assertTrue(childrenField.isPresent());
        final DataType childrenDataType = childrenField.get().getDataType();
        assertEquals(RecordFieldType.ARRAY.getArrayDataType(RecordFieldType.RECORD.getDataType()), childrenDataType);
        final DataType childrenElementDataType = ((ArrayDataType) childrenDataType).getElementType();
        assertEquals(RecordFieldType.RECORD.getDataType(), childrenElementDataType);
        childSchema = ((RecordDataType) childrenElementDataType).getChildSchema();
        assertEquals("BasicTypes", childSchema.getSchemaName().orElse(null));
        assertEquals("org.apache.nifi.jasn1.example", childSchema.getSchemaNamespace().orElse(null));

    }


    @Test
    public void testRecursive() {
        final RecordSchemaProvider provider = new RecordSchemaProvider();
        final RecordSchema schema = provider.get(Recursive.class);

        // Parent should be a record.
        final Optional<RecordField> parentField = schema.getField("parent");
        assertTrue(parentField.isPresent());
        final DataType childDataType = parentField.get().getDataType();
        assertEquals(RecordFieldType.RECORD.getDataType(), childDataType);
        final RecordSchema parentSchema = ((RecordDataType) childDataType).getChildSchema();
        assertEquals("Recursive", parentSchema.getSchemaName().orElse(null));
        assertEquals("org.apache.nifi.jasn1.example", parentSchema.getSchemaNamespace().orElse(null));

        // Children should be an array of records.
        final Optional<RecordField> childrenField = schema.getField("children");
        assertTrue(childrenField.isPresent());
        final DataType childrenDataType = childrenField.get().getDataType();
        assertEquals(RecordFieldType.ARRAY.getArrayDataType(RecordFieldType.RECORD.getDataType()), childrenDataType);
        final DataType childrenElementDataType = ((ArrayDataType) childrenDataType).getElementType();
        assertEquals(RecordFieldType.RECORD.getDataType(), childrenElementDataType);
        final RecordSchema childSchema = ((RecordDataType) childrenElementDataType).getChildSchema();
        assertEquals("Recursive", childSchema.getSchemaName().orElse(null));
        assertEquals("org.apache.nifi.jasn1.example", childSchema.getSchemaNamespace().orElse(null));

    }
}
