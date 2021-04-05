/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pulsar.io.elasticsearch;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.ByteArrayOutputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Map;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.schema.GenericObject;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.client.impl.schema.KeyValueSchema;
import org.apache.pulsar.client.impl.schema.generic.GenericAvroRecord;
import org.apache.pulsar.client.impl.schema.generic.GenericJsonRecord;
import org.apache.pulsar.common.schema.KeyValue;
import org.apache.pulsar.common.schema.SchemaType;
import org.apache.pulsar.functions.api.KVRecord;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.core.Sink;
import org.apache.pulsar.io.core.SinkContext;
import org.apache.pulsar.io.core.annotations.Connector;
import org.apache.pulsar.io.core.annotations.IOType;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.*;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.JsonEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.generic.GenericDatumWriter;

/**
 * The base abstract class for ElasticSearch sinks.
 * Users need to implement extractKeyValue function to use this sink.
 * This class assumes that the input will be JSON documents
 */
@Connector(
    name = "elastic_search",
    type = IOType.SINK,
    help = "A sink connector that sends pulsar messages to elastic search",
    configClass = ElasticSearchConfig.class
)
@Slf4j
public class ElasticSearchSink implements Sink<GenericObject> {

    private ElasticSearchConfig elasticSearchConfig;
    private ElasticsearchClient elasticsearchClient;
    private ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public void open(Map<String, Object> config, SinkContext sinkContext) throws Exception {
        elasticSearchConfig = ElasticSearchConfig.load(config);
        elasticSearchConfig.validate();
        elasticsearchClient = new ElasticsearchClient(elasticSearchConfig);
    }

    @Override
    public void close() throws Exception {
        if (elasticsearchClient != null) {
            elasticsearchClient.close();
            elasticsearchClient = null;
        }
    }

    @VisibleForTesting
    void setElasticsearchClient(ElasticsearchClient elasticsearchClient) {
        this.elasticsearchClient = elasticsearchClient;
    }

    @Override
    public void write(Record<GenericObject> record) {
        try {
            System.out.println("Writing record.value=" + record.getValue());
            Pair<String, String> idAndDoc = extractIdAndDocument(record);
            if(idAndDoc.getRight() == null) {
                elasticsearchClient.deleteDocument(record, idAndDoc);
            } else {
                elasticsearchClient.indexDocument(record, idAndDoc);
            }
        } catch(Exception e) {
            System.out.println("Unexpected error " + e);
            e.printStackTrace();
        }
    }

    /**
     * Extract ES _id and _source using the Schema if available.
     * @param record
     * @return A pair for _id and _source
     */
    public Pair<String, String> extractIdAndDocument(Record<GenericObject> record) {
        Object key = null;
        Object value = null;
        Schema<?> keySchema = null;
        Schema<?> valueSchema = null;

        System.out.println(" schema=" + record.getSchema());
        System.out.println(" value="+record.getValue());
        System.out.println(" schemaType=" + record.getValue().getSchemaType());
        if (SchemaType.KEY_VALUE.equals(record.getValue().getSchemaType())) {
            key = ((KeyValue) record.getValue().getNativeObject()).getKey();
            keySchema = ((KeyValueSchema) record.getSchema()).getKeySchema();
            value = ((KeyValue) record.getValue().getNativeObject()).getValue();
            valueSchema = ((KeyValueSchema) record.getSchema()).getValueSchema();
        } else {
            value = record.getValue().getNativeObject();
            valueSchema = record.getSchema();
            key = record.getKey().orElse(null);
        }

        String id = key + "";
        if (keySchema != null) {
            key = stringify(keySchema, key);
        }

        String doc = null;
        if (value != null) {
            if (valueSchema != null) {
                doc = stringify(valueSchema, value);
            } else {
                doc = value.toString();
            }
        }

        if (id == null && doc != null && elasticSearchConfig.getPrimaryFields() != null) {
            // extract the PK from the JSON document
            try {
                JsonNode jsonNode = objectMapper.readTree(doc);
                StringBuffer sb = new StringBuffer("[");
                for(String field : elasticSearchConfig.getPrimaryFields().split(",")) {
                    if (sb.length() > 1)
                        sb.append(",");
                    sb.append(jsonNode.get(field));
                }
                id = sb.append("]").toString();
            } catch(JsonProcessingException e) {
                System.out.println("Failed to read JSON" + e.toString());
                e.printStackTrace();
            }
        }

        SchemaType schemaType = null;
        if (record.getSchema() != null && record.getSchema().getSchemaInfo() != null) {
            schemaType = record.getSchema().getSchemaInfo().getType();
        }
        System.out.println("recordType="+ record.getClass().getName() +
                " schemaType="+ schemaType +
                " id=" + id +
                " doc=" + doc);
        return Pair.of(id, doc);
    }

    public String stringify(Schema<?> schema, Object val) {
        switch(schema.getSchemaInfo().getType()) {
            case INT8:
                return Byte.toString((Byte)val);
            case INT16:
                return  Short.toString((Short)val);
            case INT32:
                return  Integer.toString((Integer)val);
            case INT64:
                return  Long.toString((Long)val);
            case STRING:
                return (String) val;
            case JSON:
                try {
                    GenericJsonRecord genericJsonRecord = (GenericJsonRecord) val;
                    return objectMapper.writeValueAsString(genericJsonRecord.getJsonNode());
                } catch(JsonProcessingException e) {
                    throw new RuntimeException(e);
                }
            case AVRO:
                try {
                    GenericAvroRecord genericAvroRecord = (GenericAvroRecord) val;
                    ByteArrayOutputStream baos = new ByteArrayOutputStream();
                    writeAsJson(genericAvroRecord.getAvroRecord(), baos);
                    return baos.toString();
                } catch(Exception e) {
                    e.printStackTrace();
                }
            default:
                throw new UnsupportedOperationException();
        }
    }

    /**
     * Writes provided {@link org.apache.avro.generic.GenericRecord} into the provided
     * {@link OutputStream} as JSON.
     */
    public static void writeAsJson(org.apache.avro.generic.GenericRecord record, OutputStream out) throws Exception {
        DatumWriter<org.apache.avro.generic.GenericRecord> writer =
                new GenericDatumWriter<org.apache.avro.generic.GenericRecord>(record.getSchema());
        JsonEncoder encoder = EncoderFactory.get().jsonEncoder(record.getSchema(), out);
        writer.write(record, encoder);
        encoder.flush();
    }
}
