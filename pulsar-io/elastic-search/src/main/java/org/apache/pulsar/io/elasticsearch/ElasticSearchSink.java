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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.pulsar.client.api.schema.Field;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.common.schema.SchemaInfo;
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
import java.util.Arrays;
import java.util.Map;
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
public class ElasticSearchSink implements Sink<byte[]> {

    protected static final String ACTION = "ACTION";
    protected static final String INSERT = "INSERT";
    protected static final String UPDATE = "UPDATE";
    protected static final String DELETE = "DELETE";

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
    public void write(Record<byte[]> record) {
        String action = record.getProperties().get(ACTION);
        if (action == null) {
            action = INSERT;
        }
        switch(action) {
            case DELETE:
                deleteDocument(record);
                break;
            case UPDATE:
            case INSERT:
                indexDocument(record);
                break;
            default:
                String msg = String.format("Unsupported action %s, can be one of %s, or not set which indicate %s",
                        action, Arrays.asList(INSERT, UPDATE, DELETE), INSERT);
                throw new IllegalArgumentException(msg);
        }
    }

    void indexDocument(Record<byte[]> record) {
        Pair<String, String> document = extractDocument(record);
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

    void deleteDocument(Record<byte[]> record) {
        Pair<String, String> document = extractDocument(record);
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
    public Pair<String, String> extractDocument(Record<byte[]> record) {
        String id = null,  doc = null;
        SchemaInfo keySchemaInfo = record.getSchema().getSchemaInfo();
        switch(keySchemaInfo.getType()) {
            case AVRO:
                // TODO: support AVRO
                break;
            case BYTES:
            case JSON:
                doc = new String(record.getValue());
                StringBuffer sb = new StringBuffer("[");
                try {
                    JsonNode actualObj = objectMapper.readTree(doc);
                    int i = 0;
                    for(String pkField : elasticSearchConfig.getPrimaryFields().split(",")) {
                        if (i > 0)
                            sb.append(",");
                        sb.append(actualObj.get(pkField).toString());
                        i++;
                    }
                } catch(IOException e) {
                    e.printStackTrace();
                }
                sb.append("]");
                id = sb.toString();
                break;
        }
        System.out.println("schemaType="+keySchemaInfo.getType()+" properties="+record.getProperties()+" id="+id+" doc="+doc);
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
