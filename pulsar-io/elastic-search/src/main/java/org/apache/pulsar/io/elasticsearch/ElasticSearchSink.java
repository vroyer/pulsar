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
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.schema.KeyValueSchema;
import org.apache.pulsar.client.impl.schema.generic.GenericJsonRecord;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.core.Sink;
import org.apache.pulsar.io.core.SinkContext;
import org.apache.pulsar.io.core.annotations.Connector;
import org.apache.pulsar.io.core.annotations.IOType;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.*;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Map;
import java.util.Optional;

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

    private URL url;
    private RestHighLevelClient client;
    private CredentialsProvider credentialsProvider;
    private ElasticSearchConfig elasticSearchConfig;
    private ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public void open(Map<String, Object> config, SinkContext sinkContext) throws Exception {
        elasticSearchConfig = ElasticSearchConfig.load(config);
        elasticSearchConfig.validate();
        createIndexIfNeeded();
    }

    @Override
    public void close() throws Exception {
        client.close();
    }

    @Override
    public void write(Record<Object> record) {
        try {
            System.out.println("Writing record.value=" + record.getValue());
            Pair<String, String> idAndDoc = extractIdAndDocument(record);
            if(idAndDoc.getValue() == null) {
                deleteDocument(record, idAndDoc);
            } else {
                indexDocument(record, idAndDoc);
            }
        } catch(Exception e) {
            log.error("Unexpected error", e);
        }
    }

    void indexDocument(Record<Object> record, Pair<String, String> document) {
        IndexRequest indexRequest = Requests.indexRequest(elasticSearchConfig.getIndexName());
        if (document.getLeft() != null) {
            indexRequest.id(document.getLeft());
        }
        indexRequest.type(elasticSearchConfig.getTypeName());
        indexRequest.source(document.getRight(), XContentType.JSON);

        try {
            IndexResponse indexResponse = getClient().index(indexRequest, RequestOptions.DEFAULT);
            System.out.println("index result=" + indexResponse.getResult());
            if (indexResponse.getResult().equals(DocWriteResponse.Result.CREATED) ||
                    indexResponse.getResult().equals(DocWriteResponse.Result.UPDATED)) {
                record.ack();
            } else {
                record.fail();
            }
        } catch (final Exception ex) {
            ex.printStackTrace();
            record.fail();
        }
    }

    void deleteDocument(Record<Object> record, Pair<String, String> document) {
        DeleteRequest deleteRequest = Requests.deleteRequest(elasticSearchConfig.getIndexName());
        deleteRequest.id(document.getLeft());
        deleteRequest.type(elasticSearchConfig.getTypeName());
        try {
            DeleteResponse deleteResponse = getClient().delete(deleteRequest, RequestOptions.DEFAULT);
            System.out.println("delete result=" + deleteResponse.getResult());
            if (deleteResponse.getResult().equals(DocWriteResponse.Result.DELETED) ||
                    deleteResponse.getResult().equals(DocWriteResponse.Result.NOT_FOUND)) {
                record.ack();
            } else {
                record.fail();
            }
        } catch (final Exception ex) {
            ex.printStackTrace();
            record.fail();
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

        System.out.println("recordType="+record.getClass().getName() +
                        " schemaType="+record.getSchema().getSchemaInfo().getType() +
                        " id=" + id +
                        " doc=" + doc);
        return Pair.of(id, doc);
    }

    private void createIndexIfNeeded() throws IOException {
        GetIndexRequest request = new GetIndexRequest();
        request.indices(elasticSearchConfig.getIndexName());
        boolean exists = getClient().indices().exists(request, RequestOptions.DEFAULT);

        if (!exists) {
            CreateIndexRequest cireq = new CreateIndexRequest(elasticSearchConfig.getIndexName());

            cireq.settings(Settings.builder()
               .put("index.number_of_shards", elasticSearchConfig.getIndexNumberOfShards())
               .put("index.number_of_replicas", elasticSearchConfig.getIndexNumberOfReplicas()));

            CreateIndexResponse ciresp = getClient().indices().create(cireq, RequestOptions.DEFAULT);
            if (!ciresp.isAcknowledged() || !ciresp.isShardsAcknowledged()) {
                throw new RuntimeException("Unable to create index.");
            }
        }
    }

    private URL getUrl() throws MalformedURLException {
        if (url == null) {
            url = new URL(elasticSearchConfig.getElasticSearchUrl());
        }
        return url;
    }

    private CredentialsProvider getCredentialsProvider() {

        if (StringUtils.isEmpty(elasticSearchConfig.getUsername())
            || StringUtils.isEmpty(elasticSearchConfig.getPassword())) {
            return null;
        }

        credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY,
                new UsernamePasswordCredentials(elasticSearchConfig.getUsername(),
                        elasticSearchConfig.getPassword()));
        return credentialsProvider;
    }

    private RestHighLevelClient getClient() throws MalformedURLException {
        if (client == null) {
          CredentialsProvider cp = getCredentialsProvider();
          RestClientBuilder builder = RestClient.builder(new HttpHost(getUrl().getHost(),
                  getUrl().getPort(), getUrl().getProtocol()));

          if (cp != null) {
              builder.setHttpClientConfigCallback(httpClientBuilder ->
              httpClientBuilder.setDefaultCredentialsProvider(cp));
          }
          client = new RestHighLevelClient(builder);
        }
        return client;
    }
}
