package org.apache.pulsar.io.core.transform;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.apache.avro.generic.GenericData;
import org.apache.avro.reflect.Nullable;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.schema.Field;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.client.api.schema.GenericRecordBuilder;
import org.apache.pulsar.client.api.schema.SchemaInfoProvider;
import org.apache.pulsar.common.schema.KeyValue;
import org.apache.pulsar.common.schema.KeyValueEncodingType;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;
import org.apache.pulsar.functions.api.KVRecord;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.core.Transformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.CompletableFuture;

public class RenameFields implements Transformation<Object> {
    private static final Logger LOG = LoggerFactory.getLogger(RenameFields.class);

    Transformation.Type type = Type.VALUE;
    List<String> sources = new ArrayList<>();
    List<String> targets = new ArrayList<>();

    @NoArgsConstructor
    @AllArgsConstructor
    @Getter
    class SchemaAndVersion {
        Schema schema;
        byte[] schemaVersion;
        org.apache.avro.Schema keyOrValueAvroSchema;
    }

    // record wrapper with the replaced schema and value
    private class MyRecord implements Record
    {
        private final Record record;
        private final Schema schema;
        private final Object value;
        private final Type type;

        public MyRecord(Record record, Schema schema, Object value, Type type) {
            this.record = record;
            this.schema = schema;
            this.value = value;
            this.type = type;
        }

        @Override
        public Optional<String> getTopicName() {
            return record.getTopicName();
        }

        @Override
        public Optional<String> getKey() {
            return type.equals(Type.KEY) ? Optional.of((String)value) : record.getKey();
        }

        public Schema getSchema() {
            return schema;
        }

        @Override
        public Object getValue() {
            return type.equals(Type.VALUE) ? value : record.getValue();
        }

        /**
         * Acknowledge that this record is fully processed.
         */
        public void ack() {
            record.ack();
        }

        /**
         * To indicate that this record has failed to be processed.
         */
        public void fail() {
            record.fail();
        }

        /**
         * To support message routing on a per message basis.
         *
         * @return The topic this message should be written to
         */
        public Optional<String> getDestinationTopic() {
            return record.getDestinationTopic();
        }

        public Optional<Message> getMessage() {
            return record.getMessage();
        }
    }

    /**
     * Last processed output pulsar Schema
     */
    SchemaAndVersion lastSchemaAndVersion;

    @Override
    public Type type() {
        return type;
    }

    @Override
    public void init(Map<String, Object> config) throws Exception
    {
        if (config.containsKey("type") && ((String)config.get("type")).equalsIgnoreCase("key")) {
            type = Type.KEY;
        }
        if (config.containsKey("renames")) {
            for(String rename : ((String)config.get("renames")).split(",")) {
                String[] parts = rename.split(":");
                if (parts.length == 2 && parts[0].length() > 0 && parts[1].length() > 0) {
                    sources.add(parts[0]);
                    targets.add(parts[0]);
                }
            }
        }
        LOG.debug("type={} sources={} targets={}", type, sources, targets);
    }

    /**
     * Build the output schema
     * @param inputSchema
     * @return
     */
    org.apache.avro.Schema maybeUpdateAvroSchema(org.apache.avro.Schema inputSchema, byte[] schemaVersion) {
        return inputSchema;
    }

    TreeMap<String, Object> flatten(TreeMap<String, Object> map, String path, org.apache.avro.generic.GenericRecord genericRecord) {
        for(org.apache.avro.Schema.Field field : genericRecord.getSchema().getFields()) {
            Object value = genericRecord.get(field.name());
            String key = path.length() > 0 ? path + "." + field.name() : field.name();
            if (value instanceof org.apache.avro.generic.GenericRecord) {
                flatten(map, key, (org.apache.avro.generic.GenericRecord) value);
            } else {
                map.put(key, value);
            }
        }
        return map;
    }

    org.apache.avro.generic.GenericRecord rebuid(org.apache.avro.Schema schema, TreeMap<String, Object> map, String prefix) {
        org.apache.avro.generic.GenericRecordBuilder genericRecordBuilder = new org.apache.avro.generic.GenericRecordBuilder(schema);
        for(String path : map.subMap(prefix, true, prefix, true ).keySet()) {
            if (path.length() > prefix.length())
            {
                String subpath = path.substring(prefix.length() + 1);
                genericRecordBuilder.set(subpath, rebuid(schema.getField(subpath).schema(), map, path));
            } else {
                genericRecordBuilder.set(path, map.get(path));
            }
        }
        return genericRecordBuilder.build();
    }

    @Override
    public Record apply(Record<Object> record) {
        //  update the local schema if obsolete
        Object object = type.equals(Type.VALUE) ? record.getValue() : record.getKey();
        if (object  instanceof GenericRecord) {
            GenericRecord input = (GenericRecord) object;
            if (input.getSchemaType() == SchemaType.AVRO) {
                org.apache.avro.generic.GenericRecord inGenericRecord = (org.apache.avro.generic.GenericRecord) input.getNativeObject();
                org.apache.avro.Schema avroSchema = maybeUpdateAvroSchema(inGenericRecord.getSchema(), record.getMessage().isPresent() ? record.getMessage().get().getSchemaVersion() : null);
                TreeMap props = new TreeMap<>();
                flatten(props, "", inGenericRecord);
                for(int i = 0; i < sources.size(); i++)
                {
                    Object value = props.remove(sources.get(i));
                    if (value != null)
                        props.put(targets.get(i), value);
                }
                org.apache.avro.generic.GenericRecord outGenericRecord = rebuid(avroSchema, props, "");
                return new MyRecord(record, new AvroSchemaWrapper(avroSchema), outGenericRecord, type);
            }
        }
        return record;
    }

    @Override
    public void close() {

    }

    @Override
    public boolean test(Record<Object> objectRecord) {
        return true;
    }
}
