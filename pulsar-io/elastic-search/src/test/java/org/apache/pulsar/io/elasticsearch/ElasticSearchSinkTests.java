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

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import net.andreinc.mockneat.MockNeat;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.reflect.ReflectData;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.schema.GenericObject;
import org.apache.pulsar.common.schema.KeyValue;
import org.apache.pulsar.common.schema.KeyValueEncodingType;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.core.SinkContext;
import org.apache.pulsar.io.elasticsearch.data.Profile;
import org.apache.pulsar.io.elasticsearch.data.UserProfile;
import org.elasticsearch.ElasticsearchStatusException;
import org.junit.AfterClass;
import org.mockito.Mock;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.testcontainers.elasticsearch.ElasticsearchContainer;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.ByteArrayOutputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.mockito.Mockito.*;

public class ElasticSearchSinkTests {

    public final static String ELASTICSEARCH_IMAGE = "docker.elastic.co/elasticsearch/elasticsearch-oss:7.10.2-amd64";

    protected static MockNeat mockNeat;
    protected static Gson gson;
    private static ElasticsearchContainer container;

    @Mock
    protected Record<GenericObject> mockRecord;

    @Mock
    protected SinkContext mockSinkContext;
    protected Map<String, Object> map;
    protected ElasticSearchSink sink;

    @BeforeClass
    public static final void initBeforeClass() {
        mockNeat = MockNeat.threadLocal();
        gson = new GsonBuilder()
                .setPrettyPrinting()
                .create();
        container = new ElasticsearchContainer(ELASTICSEARCH_IMAGE);
    }

    @AfterClass
    public static void closeAfterClass() {
        container.close();
    }

    @SuppressWarnings("unchecked")
    @BeforeMethod
    public final void setUp() throws Exception {
        container.start();

        map = new HashMap<String, Object> ();
        map.put("elasticSearchUrl", "http://"+container.getHttpHostAddress());
        sink = new ElasticSearchSink();

        mockRecord = mock(Record.class);
        mockSinkContext = mock(SinkContext.class);

        when(mockRecord.getKey()).thenAnswer(new Answer<Optional<String>>() {
            long sequenceCounter = 0;
            public Optional<String> answer(InvocationOnMock invocation) throws Throwable {
                return Optional.of( "key-" + sequenceCounter++);
            }});

        when(mockRecord.getValue()).thenAnswer(new Answer<String>() {
            public String answer(InvocationOnMock invocation) throws Throwable {
                return getJSON();
            }});

        when(mockRecord.getSchema()).thenAnswer(new Answer<Schema<KeyValue<String,String>>>() {
            public Schema<KeyValue<String,String>> answer(InvocationOnMock invocation) throws Throwable {
                return Schema.KeyValue(Schema.STRING, Schema.STRING, KeyValueEncodingType.SEPARATED);
            }});
    }

    @AfterMethod(alwaysRun = true)
    public final void tearDown() throws Exception {
        if (sink != null)
            sink.close();
    }

    @Test(enabled = true, expectedExceptions = ElasticsearchStatusException.class)
    public final void invalidIndexNameTest() throws Exception {
        map.put("indexName", "myIndex");
        sink.open(map, mockSinkContext);
        send(1);
    }

    @Test(enabled = true)
    public final void createIndexTest() throws Exception {
        map.put("indexName", "test-index");
        sink.open(map, mockSinkContext);
        send(1);
    }

    @Test(enabled = true)
    public final void singleRecordTest() throws Exception {
        map.put("indexName", "test-index");
        sink.open(map, mockSinkContext);
        send(1);
        verify(mockRecord, times(1)).ack();
    }

    @Test(enabled = true)
    public final void send100Test() throws Exception {
        map.put("indexName", "test-index");
        sink.open(map, mockSinkContext);
        send(100);
        verify(mockRecord, times(100)).ack();
    }

    protected final void send(int numRecords) throws Exception {
        for (int idx = 0; idx < numRecords; idx++) {
            sink.write(mockRecord);
        }
    }

    private static String getJSON() {
        return mockNeat
                .reflect(UserProfile.class)
                .field("name", mockNeat.names().full())
                .field("userName", mockNeat.users())
                .field("email", mockNeat.emails())
                .field("profiles",
                        mockNeat.reflect(Profile.class)
                                .field("profileId", mockNeat.ints().range(100, 1000))
                                .field("profileAdded", mockNeat.localDates().toUtilDate())
                                .list(2))
                .map(gson::toJson)
                .val();
    }

}