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

package org.apache.samza.container;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import org.apache.samza.SamzaException;
import org.apache.samza.config.Config;
import org.apache.samza.config.JobConfig;
import org.apache.samza.config.MapConfig;
import org.apache.samza.container.grouper.task.TaskAssignmentManager;
import org.apache.samza.coordinator.stream.messages.CoordinatorStreamMessage;
import org.apache.samza.coordinator.stream.messages.SetChangelogMapping;
import org.apache.samza.coordinator.stream.messages.SetTaskContainerMapping;
import org.apache.samza.metadatastore.MetadataStore;
import org.apache.samza.metadatastore.MetadataStoreFactory;
import org.apache.samza.metrics.MetricsRegistry;
import org.apache.samza.serializers.JsonSerde;
import org.apache.samza.serializers.Serde;
import org.apache.samza.util.Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import org.apache.samza.coordinator.stream.messages.SetContainerHostMapping;

/**
 * Locality Manager is used to persist and read the container-to-host
 * assignment information from the coordinator stream
 * */
public class LocalityManager {
  private static final String SOURCE = "SamzaContainer";
  private static final Logger LOG = LoggerFactory.getLogger(LocalityManager.class);

  private final Config config;
  private final MetadataStore metadataStore;
  private Map<String, Map<String, String>> containerToHostMapping = new HashMap<>();
  private final TaskAssignmentManager taskAssignmentManager;
  private final Serde<String> keySerde;
  private final Serde<String> valueSerde;

  public LocalityManager(Config config, MetricsRegistry metricsRegistry) {
    this(new MapConfig(config, ImmutableMap.of("coordinator.stream.message.type", SetContainerHostMapping.TYPE)),
         metricsRegistry,
         new CoordinatorStreamKeySerde(SetContainerHostMapping.TYPE),
         new CoordinatorStreamValueSerde(config, SetContainerHostMapping.TYPE));
  }

  /**
   * Constructor that creates a read-write or write-only locality manager.
   *
   * @param config Coordinator stream manager.
   */
  public LocalityManager(Config config, MetricsRegistry metricsRegistry, Serde<String> keySerde, Serde<String> valueSerde) {
    this.config = config;
    MetadataStoreFactory metadataStoreFactory = Util.getObj(new JobConfig(config).getMetadataStoreFactory());
    this.metadataStore = metadataStoreFactory.getMetadataStore(config, metricsRegistry);
    this.keySerde = keySerde;
    this.valueSerde = valueSerde;
    this.taskAssignmentManager = new TaskAssignmentManager(config, metricsRegistry, keySerde, valueSerde);
  }

  public void init(SamzaContainerContext containerContext) {
    this.metadataStore.init(containerContext);
  }

  /**
   * Method to allow read container locality information from coordinator stream. This method is used
   * in {@link org.apache.samza.coordinator.JobModelManager}.
   *
   * @return the map of containerId: (hostname, jmxAddress, jmxTunnelAddress)
   */
  public Map<String, Map<String, String>> readContainerLocality() {
    Map<String, Map<String, String>> allMappings = new HashMap<>();
    metadataStore.all().forEach((keyBytes, valueBytes) -> {
        String locationId = valueSerde.fromBytes(valueBytes);
        allMappings.put(keySerde.fromBytes(keyBytes), ImmutableMap.of(SetContainerHostMapping.HOST_KEY, locationId));
      });
    containerToHostMapping = Collections.unmodifiableMap(allMappings);

    if (LOG.isDebugEnabled()) {
      for (Map.Entry<String, Map<String, String>> entry : containerToHostMapping.entrySet()) {
        LOG.debug(String.format("Locality for container %s: %s", entry.getKey(), entry.getValue()));
      }
    }

    return allMappings;
  }

  public void close() {
    metadataStore.close();
  }

  public TaskAssignmentManager getTaskAssignmentManager() {
    return taskAssignmentManager;
  }

  /**
   * Method to write locality info to coordinator stream. This method is used in {@link SamzaContainer}.
   *
   * @param containerId  the {@link SamzaContainer} ID
   * @param hostName  the hostname
   * @param jmxAddress  the JMX URL address
   * @param jmxTunnelingAddress  the JMX Tunnel URL address
   */
  public void writeContainerToHostMapping(String containerId, String hostName, String jmxAddress, String jmxTunnelingAddress) {
    Map<String, String> existingMappings = containerToHostMapping.get(containerId);
    String existingHostMapping = existingMappings != null ? existingMappings.get(SetContainerHostMapping.HOST_KEY) : null;
    if (existingHostMapping != null && !existingHostMapping.equals(hostName)) {
      LOG.info("Container {} moved from {} to {}", new Object[]{containerId, existingHostMapping, hostName});
    } else {
      LOG.info("Container {} started at {}", containerId, hostName);
    }

    metadataStore.put(keySerde.toBytes(containerId), valueSerde.toBytes(hostName));

    Map<String, String> mappings = new HashMap<>();
    mappings.put(SetContainerHostMapping.HOST_KEY, hostName);
    mappings.put(SetContainerHostMapping.JMX_URL_KEY, jmxAddress);
    mappings.put(SetContainerHostMapping.JMX_TUNNELING_URL_KEY, jmxTunnelingAddress);
    containerToHostMapping.put(containerId, mappings);
  }

  public static class CoordinatorStreamKeySerde implements Serde<String> {

    private final Serde<List<?>> keySerde;
    private final String type;

    public CoordinatorStreamKeySerde(String type) {
      this.type = type;
      this.keySerde = new JsonSerde<>();
    }

    @Override
    public String fromBytes(byte[] bytes) {
      CoordinatorStreamMessage message = new CoordinatorStreamMessage(keySerde.fromBytes(bytes).toArray(), new HashMap<>());
      return message.getKey();
    }

    @Override
    public byte[] toBytes(String key) {
      Object[] keyArray = new Object[]{CoordinatorStreamMessage.VERSION, type, key};
      return keySerde.toBytes(Arrays.asList(keyArray));
    }
  }

  public static class CoordinatorStreamValueSerde implements Serde<String> {

    private final String type;
    private final Serde<Map<String, Object>> messageSerde;
    private final Config config;

    public CoordinatorStreamValueSerde(Config config, String type) {
      Preconditions.checkNotNull(type);
      this.config = config;
      this.type = type;
      messageSerde = new JsonSerde<>();
    }

    @Override
    public String fromBytes(byte[] bytes) {
      Map<String, Object> values = messageSerde.fromBytes(bytes);
      CoordinatorStreamMessage message = new CoordinatorStreamMessage(new Object[]{}, values);
      if (type.equalsIgnoreCase(SetContainerHostMapping.TYPE)) {
        SetContainerHostMapping hostMapping = new SetContainerHostMapping(message);
        return hostMapping.getHostLocality();
      } else if (type.equalsIgnoreCase(SetTaskContainerMapping.TYPE)) {
        SetTaskContainerMapping setTaskContainerMapping = new SetTaskContainerMapping(message);
        return setTaskContainerMapping.getTaskAssignment();
      } else if (type.equalsIgnoreCase(SetChangelogMapping.TYPE)) {
        SetChangelogMapping changelogMapping = new SetChangelogMapping(message);
        return String.valueOf(changelogMapping.getPartition());
      } else {
        throw new SamzaException(String.format("Unknown coordinator stream message type: %s", type));
      }
    }

    @Override
    public byte[] toBytes(String value) {
      if (type.equalsIgnoreCase(SetContainerHostMapping.TYPE)) {
        SetContainerHostMapping hostMapping = new SetContainerHostMapping(SOURCE, "", value, config.get("jmx.tunneling.url"), config.get("jmx.url"));
        return messageSerde.toBytes(hostMapping.getMessageMap());
      } else if (type.equalsIgnoreCase(SetTaskContainerMapping.TYPE)) {
        SetTaskContainerMapping setTaskContainerMapping = new SetTaskContainerMapping(SOURCE, "", value);
        return messageSerde.toBytes(setTaskContainerMapping.getMessageMap());
      } else if (type.equalsIgnoreCase(SetChangelogMapping.TYPE)) {
        SetChangelogMapping changelogMapping = new SetChangelogMapping(SOURCE, "", Integer.valueOf(value));
        return messageSerde.toBytes(changelogMapping.getMessageMap());
      } else {
        throw new SamzaException(String.format("Unknown coordinator stream message type: %s", type));
      }
    }
  }
}
