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
package org.apache.pulsar.client.api.schema;

import org.apache.pulsar.common.classification.InterfaceAudience;
import org.apache.pulsar.common.classification.InterfaceStability;
import org.apache.pulsar.common.schema.SchemaType;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * An interface represents a message with schema.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class PrimitiveRecord implements GenericRecord {

    private final Object nativeRecord;
    private final SchemaType schemaType;

    public static PrimitiveRecord of(Object nativeRecord, SchemaType schemaType) {
        return new PrimitiveRecord(nativeRecord, schemaType);
    }

    private PrimitiveRecord(Object nativeRecord, SchemaType schemaType) {
        this.nativeRecord = nativeRecord;
        this.schemaType = schemaType;
    }

    public byte[] getSchemaVersion() {
        return null;
    }

    public List<Field> getFields() {
        return Collections.emptyList();
    }

    public Object getField(String fieldName) {
        return null;
    }

    public SchemaType getSchemaType() {
        return schemaType;
    }

    public Object getNativeObject() {
        return nativeRecord;
    }

    @Override
    public String toString() {
        return Objects.toString(nativeRecord);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(nativeRecord);
    }

    @Override
    public boolean equals(Object other) {
        if (! (other instanceof PrimitiveRecord)) {
            return false;
        }
        return Objects.equals(nativeRecord, ((PrimitiveRecord) other).nativeRecord);
    }
}
