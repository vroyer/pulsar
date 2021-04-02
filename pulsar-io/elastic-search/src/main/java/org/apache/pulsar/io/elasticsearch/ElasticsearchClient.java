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

import com.google.common.annotations.VisibleForTesting;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.pulsar.client.api.schema.GenericObject;
import org.apache.pulsar.functions.api.Record;
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

@Slf4j
public class ElasticsearchClient {

    private URL url;
    private ElasticSearchConfig elasticSearchConfig;
    private RestHighLevelClient client;
    private CredentialsProvider credentialsProvider;
    private boolean indexChecked = false;

    ElasticsearchClient(ElasticSearchConfig elasticSearchConfig) throws IOException {
        this.elasticSearchConfig = elasticSearchConfig;
        checkIndex();
    }

    public boolean indexDocument(Record<GenericObject> record, Pair<String, String> document) {
        try {
            IndexRequest indexRequest = Requests.indexRequest(elasticSearchConfig.getIndexName());
            if (document.getLeft() != null) {
                indexRequest.id(document.getLeft());
            }
            indexRequest.type(elasticSearchConfig.getTypeName());
            indexRequest.source(document.getRight(), XContentType.JSON);
            IndexResponse indexResponse = getClient().index(indexRequest, RequestOptions.DEFAULT);
            log.debug("index result=" + indexResponse.getResult());
            if (indexResponse.getResult().equals(DocWriteResponse.Result.CREATED) ||
                    indexResponse.getResult().equals(DocWriteResponse.Result.UPDATED)) {
                record.ack();
                return true;
            } else {
                record.fail();
            }
        } catch (final Exception ex) {
            ex.printStackTrace();
            record.fail();
        }
        return false;
    }

    public boolean deleteDocument(Record<GenericObject> record, Pair<String, String> document) throws IOException {
        try {
            DeleteRequest deleteRequest = Requests.deleteRequest(elasticSearchConfig.getIndexName());
            deleteRequest.id(document.getLeft());
            deleteRequest.type(elasticSearchConfig.getTypeName());
            DeleteResponse deleteResponse = getClient().delete(deleteRequest, RequestOptions.DEFAULT);
            log.debug("delete result=" + deleteResponse.getResult());
            if (deleteResponse.getResult().equals(DocWriteResponse.Result.DELETED) ||
                    deleteResponse.getResult().equals(DocWriteResponse.Result.NOT_FOUND)) {
                record.ack();
                return true;
            } else {
                record.fail();
            }
        } catch (final Exception ex) {
            ex.printStackTrace();
            record.fail();
        }
        return false;
    }

    public void close() throws Exception {
        if (client != null)
            client.close();
    }

    private void checkIndex() throws IOException {
        if (!indexChecked) {
            synchronized(this) {
                if (!indexChecked) {
                    createIndexIfNeeded();
                    indexChecked = true;
                }
            }
        }
    }

    @VisibleForTesting
    public Void createIndexIfNeeded() throws IOException {
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
        return null;
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
