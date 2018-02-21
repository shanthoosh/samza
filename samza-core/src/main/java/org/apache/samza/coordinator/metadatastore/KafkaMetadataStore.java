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

import org.apache.samza.Partition;
import org.apache.samza.SamzaException;
import org.apache.samza.config.Config;
import org.apache.samza.container.SamzaContainerContext;
import org.apache.samza.coordinator.stream.messages.CoordinatorStreamMessage;
import org.apache.samza.metadatastore.MetadataStore;
import org.apache.samza.metrics.MetricsRegistry;
import org.apache.samza.serializers.JsonSerde;
import org.apache.samza.serializers.Serde;
import org.apache.samza.system.*;
import org.apache.samza.util.Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * An implementation of the {@link MetadataStore} interface where the
 * metadata of the Samza job is stored in coordinator stream(kafka topic).
 */
public class KafkaMetadataStore implements MetadataStore {

  private static final Logger LOG = LoggerFactory.getLogger(KafkaMetadataStore.class);
  private final Config config;
  private final SystemStream coordinatorSystemStream;
  private final SystemStreamPartition coordinatorSystemStreamPartition;
  private final SystemProducer systemProducer;
  private final SystemConsumer systemConsumer;
  private final SystemAdmin systemAdmin;
  private final Object bootstrapLock = new Object();
  private final String type;
  private final Serde<List<?>> keySerde;
  private final SystemStreamPartitionIterator iterator;
  private Map<byte[], byte[]> bootstrappedMessages = new HashMap<>();

  private SamzaContainerContext containerContext;
  private String source;

  private final AtomicBoolean isStarted = new AtomicBoolean(false);

  public KafkaMetadataStore(Config config, MetricsRegistry metricsRegistry) {
    SystemFactory systemFactory = Util.getCoordinatorSystemFactory(config);
    this.config = config;
    this.type = config.get("coordinator.stream.message.type");
    this.keySerde = new JsonSerde<>();
    this.coordinatorSystemStream = Util.getCoordinatorSystemStream(config);
    this.coordinatorSystemStreamPartition = new SystemStreamPartition(coordinatorSystemStream, new Partition(0));
    this.systemProducer = systemFactory.getProducer(this.coordinatorSystemStream.getSystem(), config, metricsRegistry);
    this.systemConsumer = systemFactory.getConsumer(this.coordinatorSystemStream.getSystem(), config, metricsRegistry);
    this.systemAdmin = systemFactory.getAdmin(this.coordinatorSystemStream.getSystem(), config);
    this.iterator = new SystemStreamPartitionIterator(systemConsumer, coordinatorSystemStreamPartition);
  }

  @Override
  public void init(SamzaContainerContext containerContext) {
    if (isStarted.compareAndSet(false, true)) {
      LOG.info("Starting the coordinator stream system consumer with config: {}.", config);
      this.containerContext = containerContext;
      this.source = String.format("SamzaContainer-%s", this.containerContext.id);
      systemConsumer.start();
      systemProducer.start();
      systemProducer.register(source);
      registerConsumer();
      bootstrapMessagesFromStream();
    } else {
      LOG.info("Coordinator stream partition {} has already been registered. Skipping.", coordinatorSystemStreamPartition);
    }
  }

  private void registerConsumer() {
    LOG.debug("Attempting to register: {}", coordinatorSystemStreamPartition);
    Set<String> streamNames = new HashSet<String>();
    String streamName = coordinatorSystemStreamPartition.getStream();
    streamNames.add(streamName);
    Map<String, SystemStreamMetadata> systemStreamMetadataMap = systemAdmin.getSystemStreamMetadata(streamNames);
    LOG.info(String.format("Got metadata %s", systemStreamMetadataMap.toString()));

    SystemStreamMetadata systemStreamMetadata = systemStreamMetadataMap.get(streamName);

    if (systemStreamMetadata == null) {
      throw new SamzaException("Expected " + streamName + " to be in system stream metadata.");
    }

    SystemStreamMetadata.SystemStreamPartitionMetadata systemStreamPartitionMetadata = systemStreamMetadata.getSystemStreamPartitionMetadata().get(coordinatorSystemStreamPartition.getPartition());

    if (systemStreamPartitionMetadata == null) {
      throw new SamzaException("Expected metadata for " + coordinatorSystemStreamPartition + " to exist.");
    }

    String startingOffset = systemStreamPartitionMetadata.getOldestOffset();
    LOG.debug("Registering {} with offset {}", coordinatorSystemStreamPartition, startingOffset);
    systemConsumer.register(coordinatorSystemStreamPartition, startingOffset);
  }

  @Override
  public byte[] get(byte[] key) {
    bootstrapMessagesFromStream();
    return bootstrappedMessages.get(key);
  }

  @Override
  public void put(byte[] key, byte[] value) {
    try {
      OutgoingMessageEnvelope envelope = new OutgoingMessageEnvelope(coordinatorSystemStream, 0, key, value);
      bootstrappedMessages.put(key, value);
      systemProducer.send(source, envelope);
    } catch (Exception e) {
      throw new SamzaException(e);
    }
  }

  @Override
  public void remove(byte[] key) {
    // Since kafka doesn't support individual message deletion, store value as null for a key to delete.
    put(key, null);
  }

  @Override
  public Map<byte[], byte[]> all() {
    return Collections.unmodifiableMap(bootstrappedMessages);
  }

  /**
   * Read all messages from the earliest offset, all the way to the latest.
   * Currently, this method only pays attention to config messages.
   */
  private void bootstrapMessagesFromStream() {
    synchronized (bootstrapLock) {
      try {
        while (iterator.hasNext()) {
          IncomingMessageEnvelope envelope = iterator.next();
          Object[] keyArray = keySerde.fromBytes((byte[]) envelope.getKey()).toArray();
          CoordinatorStreamMessage coordinatorStreamMessage = new CoordinatorStreamMessage(keyArray, new HashMap<>());
          if (Objects.equals(coordinatorStreamMessage.getType(), type)) {
            byte[] key = (byte[]) envelope.getKey();
            byte[] value = (byte[]) envelope.getKey();
            bootstrappedMessages.put(key, value);
          }
        }
      } catch (Exception e) {
        throw new SamzaException(e);
      }
    }
  }

  @Override
  public void close() {
    try {
      LOG.info("Stopping the coordinator stream system consumer.", config);
      systemAdmin.stop();
      systemProducer.stop();
      systemConsumer.stop();
    } catch (Exception e) {
      LOG.error("Exception occurred when closing metadata store:", e);
    }
  }
}
