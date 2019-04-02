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

import java.util.Collections;
import java.util.Set;
import java.util.Map;
import java.util.HashMap;
import java.util.Objects;
import org.apache.samza.config.Config;
import org.apache.samza.coordinator.stream.CoordinatorStreamKeySerde;
import org.apache.samza.coordinator.stream.CoordinatorStreamManager;
import org.apache.samza.coordinator.stream.messages.CoordinatorStreamMessage;
import org.apache.samza.metadatastore.MetadataStore;
import org.apache.samza.metrics.MetricsRegistry;
import org.apache.samza.serializers.JsonSerde;
import org.apache.samza.serializers.Serde;
import org.apache.samza.system.SystemStream;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.util.CoordinatorStreamUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An implementation of the {@link MetadataStore} interface where the metadata of the Samza job is stored in coordinator stream.
 *
 * This class is thread safe.
 */
public class CoordinatorStreamStore implements MetadataStore {

  private static final Logger LOG = LoggerFactory.getLogger(CoordinatorStreamStore.class);
  private static final String SOURCE = "SamzaContainer";

  private final SystemStream coordinatorSystemStream;
  private final String namespace;
  private final CoordinatorStreamKeySerde coordinatorKeySerde;
  private final Serde<Map<String, Object>> coordinatorMessageSerde = new JsonSerde<>();
  private final CoordinatorStreamManager coordinatorStreamManager;
  private final Object bootstrapLock = new Object();

  private Map<String, byte[]> bootstrappedMessages = new HashMap<>();

  public CoordinatorStreamStore(String namespace, Config config, MetricsRegistry metricsRegistry) {
    this(namespace, config, metricsRegistry, new CoordinatorStreamManager(config, metricsRegistry));
  }

  public CoordinatorStreamStore(String namespace, Config config, MetricsRegistry metricsRegistry, CoordinatorStreamManager coordinatorStreamManager) {
    this.namespace = namespace;
    this.coordinatorKeySerde = new CoordinatorStreamKeySerde(this.namespace);
    this.coordinatorSystemStream = CoordinatorStreamUtil.getCoordinatorSystemStream(config);
    this.coordinatorStreamManager = coordinatorStreamManager;
  }

  @Override
  public void init() {
    coordinatorStreamManager.start();
  }

  @Override
  public byte[] get(String key) {
    bootstrapMessagesFromStream();
    return bootstrappedMessages.get(key);
  }

  @Override
  public void put(String key, byte[] value) {
    OutgoingMessageEnvelope envelope = new OutgoingMessageEnvelope(coordinatorSystemStream, 0, coordinatorKeySerde.toBytes(key), value);
    coordinatorStreamManager.send(SOURCE, envelope);
    flush();
  }

  @Override
  public void delete(String key) {
    // Since kafka doesn't support individual message deletion, store value as null for a key to delete.
    put(key, null);
  }

  @Override
  public Map<String, byte[]> all() {
    bootstrapMessagesFromStream();
    return Collections.unmodifiableMap(bootstrappedMessages);
  }

  /**
   * Returns all the messages from the earliest offset all the way to the latest.
   */
  private void bootstrapMessagesFromStream() {
    synchronized (bootstrapLock) {
      Map<String, byte[]> bootstrappedMessages = new HashMap<>();
      Set<CoordinatorStreamMessage> bootstrappedStream = coordinatorStreamManager.getBootstrappedStream(namespace);
      for (CoordinatorStreamMessage message : bootstrappedStream) {
        if (Objects.equals(message.getType(), namespace)) {
          bootstrappedMessages.put(message.getKey(), coordinatorMessageSerde.toBytes(message.getMessageMap()));
        }
      }
      this.bootstrappedMessages = bootstrappedMessages;
    }
  }

  @Override
  public void close() {
    try {
      LOG.info("Stopping the coordinator stream manager.");
      coordinatorStreamManager.stop();
    } catch (Exception e) {
      LOG.error("Exception occurred when closing the metadata store:", e);
    }
  }

  @Override
  public void flush() {
    try {
      coordinatorStreamManager.flush(SOURCE);
    } catch (Exception e) {
      LOG.error("Exception occurred when flushing the metadata store:", e);
    }
  }
}
