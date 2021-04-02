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

import com.google.common.collect.Lists;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.reflect.ReflectData;
import org.apache.pulsar.io.elasticsearch.data.Profile;
import org.apache.pulsar.io.elasticsearch.data.UserProfile;
import org.testng.annotations.Test;

import java.io.ByteArrayOutputStream;
import java.util.Date;

public class ElasticSearchTests {

    @Test
    public final void testAvroToJson() throws Exception {
        org.apache.avro.Schema schema = ReflectData.get().getSchema(UserProfile.class);
        System.out.println("schema=" + schema);

        org.apache.avro.Schema profileSchema = ReflectData.get().getSchema(Profile.class);
        GenericRecord profile = new GenericRecordBuilder(profileSchema)
                .set("profileId", 1)
                .set("profileAdded", new Date())
                .build();

        GenericRecord record = new GenericRecordBuilder(schema)
                .set("email","bob@bob.com")
                .set("name","bob")
                .set("userName","Bob")
                .set("profiles", Lists.newArrayList(profile))
                .build();
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ElasticSearchSink.writeAsJson(record, baos);
        System.out.println("record="+baos);
    }
}