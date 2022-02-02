package org.apache.pulsar.io.core.transform;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.functions.api.Record;
import org.junit.Test;

import java.util.Optional;

import static org.testng.Assert.assertEquals;

public class RenameFieldTests
{
    class TestRecord implements Record
    {
        Schema schema;
        Optional<String> key;
        Object value;

        TestRecord(Schema schema, String key, Object value) {
            this.schema = schema;
            this.key = Optional.ofNullable(key);
            this.value = value;
        }

        @Override
        public Schema getSchema()
        {
            return schema;
        }

        @Override
        public Optional<String> getKey()
        {
            return key;
        }

        @Override
        public Object getValue()
        {
            return value;
        }
    }


    @Test
    public void testRename() throws Exception
    {
        org.apache.avro.Schema schemaX = org.apache.avro.Schema.createRecord("x", "x", "ns1", false,
                ImmutableList.of(
                        new org.apache.avro.Schema.Field("x1", org.apache.avro.Schema.create(org.apache.avro.Schema.Type.STRING)),
                        new org.apache.avro.Schema.Field("x2", org.apache.avro.Schema.create(org.apache.avro.Schema.Type.INT))));
        org.apache.avro.Schema schemaY = org.apache.avro.Schema.createRecord("y", "y", "ns1", false,
                ImmutableList.of(
                        new org.apache.avro.Schema.Field("y1", org.apache.avro.Schema.create(org.apache.avro.Schema.Type.STRING)),
                        new org.apache.avro.Schema.Field("y2", org.apache.avro.Schema.create(org.apache.avro.Schema.Type.INT))));
        org.apache.avro.Schema rootSchema = org.apache.avro.Schema.createRecord("r", "r", "ns1", false,
                ImmutableList.of(
                        new org.apache.avro.Schema.Field("x", schemaX),
                        new org.apache.avro.Schema.Field("y", schemaY),
                new org.apache.avro.Schema.Field("a", org.apache.avro.Schema.create(org.apache.avro.Schema.Type.STRING))));

        org.apache.avro.generic.GenericRecord genericRecordX = new org.apache.avro.generic.GenericData.Record(schemaX);
        genericRecordX.put("x1", "xx1");
        genericRecordX.put("x2", "xx2");
        org.apache.avro.generic.GenericRecord genericRecordY = new org.apache.avro.generic.GenericData.Record(schemaY);
        genericRecordY.put("y1", "yy1");
        genericRecordY.put("y2", "yy2");
        org.apache.avro.generic.GenericRecord genericRecord = new org.apache.avro.generic.GenericData.Record(rootSchema);
        genericRecord.put("a", "aaa");
        genericRecord.put("x", genericRecordX);
        genericRecord.put("y", genericRecordY);

        TestRecord testRecord = new TestRecord(new AvroSchemaWrapper(rootSchema), "key1", genericRecord);

        RenameFields renameFields = new RenameFields();
        renameFields.init(ImmutableMap.of("type","value","renames","a:b,x.x1:x.xx1"));
        Record result = renameFields.apply(testRecord);
        org.apache.pulsar.client.api.schema.GenericRecord genericRecord1 = (org.apache.pulsar.client.api.schema.GenericRecord) result.getValue();
        org.apache.avro.generic.GenericRecord genericRecord11 = (org.apache.avro.generic.GenericRecord) genericRecord1.getNativeObject();
        assertEquals("aaa", genericRecord1.getField("b"));
        org.apache.avro.generic.GenericRecord genericRecord11X = (org.apache.avro.generic.GenericRecord) genericRecord11.get("x");
        assertEquals("xx1", genericRecord11X.get("xx1"));
    }
}
