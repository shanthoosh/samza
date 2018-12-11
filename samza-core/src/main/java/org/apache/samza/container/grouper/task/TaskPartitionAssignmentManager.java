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
package org.apache.samza.container.grouper.task;

import org.apache.samza.SamzaException;
import org.apache.samza.config.Config;
import org.apache.samza.config.JobConfig;
import org.apache.samza.config.TaskConfigJava;
import org.apache.samza.container.TaskName;
import org.apache.samza.coordinator.stream.CoordinatorStreamValueSerde;
import org.apache.samza.coordinator.stream.messages.SetTaskPartitionMapping;
import org.apache.samza.metadatastore.MetadataStore;
import org.apache.samza.metadatastore.MetadataStoreFactory;
import org.apache.samza.metrics.MetricsRegistry;
import org.apache.samza.serializers.Serde;
import org.apache.samza.serializers.model.SamzaObjectMapper;
import org.apache.samza.system.SystemStreamPartition;
import org.apache.samza.util.Util;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

/**
 * TODO: Add java doc.
 */
public class TaskPartitionAssignmentManager {

  public static final Logger LOG = LoggerFactory.getLogger(TaskPartitionAssignmentManager.class);
  private final Config config;
  private final MetricsRegistry metricsRegistry;
  private final ObjectMapper mapper = SamzaObjectMapper.getObjectMapper();
  private final Serde<String> valueSerde;
  private final MetadataStore metadataStore;
  private final Set<SystemStreamPartition> broadcastSystemStreamPartitions;

  /**
   * @param config
   * @param metricsRegistry
   */
  public TaskPartitionAssignmentManager(Config config, MetricsRegistry metricsRegistry) {
    this(config, metricsRegistry, new CoordinatorStreamValueSerde(SetTaskPartitionMapping.TYPE));
  }

  TaskPartitionAssignmentManager(Config config, MetricsRegistry metricsRegistry, Serde<String> valueSerde) {
    this.config = config;
    this.metricsRegistry = metricsRegistry;
    this.valueSerde = valueSerde;
    this.broadcastSystemStreamPartitions = new TaskConfigJava(config).getBroadcastSystemStreamPartitions();
    MetadataStoreFactory metadataStoreFactory = Util.getObj(new JobConfig(config).getMetadataStoreFactory(), MetadataStoreFactory.class);
    this.metadataStore = metadataStoreFactory.getMetadataStore(SetTaskPartitionMapping.TYPE, config, metricsRegistry);
    this.metadataStore.init();
  }

  /**
   *
   * @return
   */
  public Map<TaskName, List<SystemStreamPartition>> readTaskPartitionAssignments() {
    Map<TaskName, List<SystemStreamPartition>> taskNameToPartitions = new HashMap<>();
    metadataStore.all().forEach((partitionAsJson, taskNameAsBytes) -> {
        SystemStreamPartition systemStreamPartition = getSSPFromSerializedJson(partitionAsJson);
        String task = valueSerde.fromBytes(taskNameAsBytes);
        TaskName taskName = new TaskName(task);
        if (!taskNameToPartitions.containsKey(taskName)) {
          taskNameToPartitions.put(taskName, new ArrayList<>());
        }
        taskNameToPartitions.get(taskName).add(systemStreamPartition);
      });
    return taskNameToPartitions;
  }

  public void writeTaskPartitionAssignment(TaskName task, List<SystemStreamPartition> partitions) {
    partitions.forEach(partition -> {
        if (broadcastSystemStreamPartitions.contains(partition)) {
          LOG.info("Not storing the task assignment for the broadcast partition: {} in coordinator stream.", partition);
        } else {
          String serializedKey = getKey(partition);
          String taskName = task.getTaskName();
          metadataStore.put(serializedKey, valueSerde.toBytes(taskName));
        }
      });
  }

  public void close() {
    metadataStore.close();
  }

  private String getKey(SystemStreamPartition systemStreamPartition) {
    try {
      return mapper.writeValueAsString(systemStreamPartition);
    } catch (IOException e) {
      throw new SamzaException(
              String.format("Exception occurred when serializing the partition: %s", systemStreamPartition), e);
    }
  }

  private SystemStreamPartition getSSPFromSerializedJson(String partitionAsString) {
    try {
      return mapper.readValue(partitionAsString, SystemStreamPartition.class);
    } catch (IOException e) {
      throw new SamzaException(
              String.format("Exception occurred when deserializing the partition: %s", partitionAsString), e);
    }
  }
}
