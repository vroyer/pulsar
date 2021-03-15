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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.schema.KeyValueSchema;
import org.apache.pulsar.client.impl.schema.generic.GenericJsonRecord;
import org.apache.pulsar.common.schema.SchemaType;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.core.Sink;
import org.apache.pulsar.io.core.SinkContext;
import org.apache.pulsar.io.core.annotations.Connector;
import org.apache.pulsar.io.core.annotations.IOType;

import java.util.Map;

/**
 * The base abstract class for ElasticSearch sinks.
 * Users need to implement extractKeyValue function to use this sink.
 */
@Connector(
    name = "elastic_search",
    type = IOType.SINK,
    help = "A keyed sink connector that sends pulsar messages to elasticsearch",
    configClass = ElasticSearchConfig.class
)
@Slf4j
public class ElasticSearchSink implements Sink<Object> {

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
        if (elasticsearchClient != null)
            elasticsearchClient.close();
        elasticsearchClient = null;
    }

    @VisibleForTesting
    void setElasticsearchClient(ElasticsearchClient elasticsearchClient) {
        this.elasticsearchClient = elasticsearchClient;
    }

    @Override
    public void write(Record<Object> record) {
        try {
            System.out.println("Writing record.value=" + record.getValue());
            Pair<String, String> idAndDoc = extractIdAndDocument(record);
            if(idAndDoc.getValue() == null) {
                elasticsearchClient.deleteDocument(record, idAndDoc);
            } else {
                elasticsearchClient.indexDocument(record, idAndDoc);
            }
        } catch(Exception e) {
            log.error("Unexpected error", e);
        }
    }

    /**
     * Extract ES _id and _source using the Schema.
     * @param record
     * @return A pair for _id and _source
     */
    public Pair<String, String> extractIdAndDocument(Record<Object> record) {
        KeyValueSchema<Object,Object> keyValueSchema = record.getKeyValueSchema();

        String id = record.getKey().isPresent() ? record.getKey().get() : null;
        if (record.getRecordKey().isPresent()) {
            Object key = record.getRecordKey().get();
            Schema keySchema = keyValueSchema == null ? null : keyValueSchema.getKeySchema();
            if (keySchema != null) {
                switch(keySchema.getSchemaInfo().getType()) {
                    case STRING:
                        id = (String) key;
                        break;
                    case INT8:
                        id = Byte.toString((Byte)key);
                        break;
                    case INT16:
                        id = Short.toString((Short)key);
                        break;
                    case INT32:
                        id = Integer.toString((Integer)key);
                        break;
                    case INT64:
                        id = Long.toString((Long)key);
                        break;
                    default:
                        throw new UnsupportedOperationException();
                }
            } else {
                id = key.toString();
            }
        }

        String doc = null;
        if (record.getValue() != null) {
            Schema valueSchema = keyValueSchema == null ?  record.getSchema() : keyValueSchema.getValueSchema();
            if (valueSchema != null) {
                Object value = record.getRecordValue();
                switch(valueSchema.getSchemaInfo().getType()) {
                    case STRING:
                        doc = (String) value;
                        break;
                    case JSON:
                        GenericJsonRecord genericJsonRecord = (GenericJsonRecord) value;
                        try {
                            doc = objectMapper.writeValueAsString(genericJsonRecord.getJsonNode());
                        } catch(JsonProcessingException e) {
                            log.error("Failed to write GenericJsonRecord as String", e);
                        }
                        break;
                    default:
                        throw new UnsupportedOperationException();
                }
            } else {
                doc = record.getValue().toString();
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
                log.error("Failed to read JSON", e);
            }
        }

        SchemaType schemaType = null;
        if (record.getSchema() != null && record.getSchema().getSchemaInfo() != null) {
            schemaType = record.getSchema().getSchemaInfo().getType();
        }
        log.debug("recordType="+ record.getClass().getName() +
                " schemaType="+ schemaType +
                " id=" + id +
                " doc=" + doc);
        return Pair.of(id, doc);
    }

}
