/*
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
package org.apache.samza.coordinator.metadatastore;

import org.apache.samza.coordinator.stream.messages.CoordinatorStreamMessage;
import org.apache.samza.metadatastore.MetadataStore;
import org.apache.samza.serializers.JsonSerde;
import org.apache.samza.serializers.Serde;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class TypedMetadataStore implements MetadataStore {

    private final MetadataStore metadataStore;
    private final String namespace;

    public TypedMetadataStore(MetadataStore metadataStore, String namespace) {
        this.metadataStore = metadataStore;
        this.namespace = namespace;
    }

    @Override
    public void init() {
        metadataStore.init();
    }

    @Override
    public byte[] get(byte[] key) {
        return metadataStore.get(key);
    }

    @Override
    public void put(byte[] key, byte[] value) {
        metadataStore.put(key, value);
    }

    @Override
    public void delete(byte[] key) {
        metadataStore.delete(key);
    }

    @Override
    public Map<byte[], byte[]> all() {
        Map<byte[], byte[]> all = metadataStore.all();
        Map<byte[], byte[]> result = new HashMap<>();
        for(Map.Entry<byte[], byte[]> entry: all.entrySet()) {
            byte[] keyAsBytes = entry.getKey();
            Serde<List<?>> keySerde = new JsonSerde<>();
            Object[] keyArray = keySerde.fromBytes(keyAsBytes).toArray();
            CoordinatorStreamMessage coordinatorStreamMessage = new CoordinatorStreamMessage(keyArray, new HashMap<>());
            if (Objects.equals(coordinatorStreamMessage.getType(), namespace)) {
                result.put(entry.getKey(), entry.getValue());
            }
        }
        return result;
    }

    @Override
    public void flush() {
        metadataStore.flush();
    }

    @Override
    public void close() {
        metadataStore.close();
    }
}
