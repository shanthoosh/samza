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

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import com.google.common.collect.ImmutableMap;
import org.apache.samza.config.Config;
import org.apache.samza.config.JobConfig;
import org.apache.samza.config.MapConfig;
import org.apache.samza.container.LocalityManager;
import org.apache.samza.container.SamzaContainerContext;
import org.apache.samza.container.TaskName;
import org.apache.samza.coordinator.stream.messages.SetTaskContainerMapping;
import org.apache.samza.metadatastore.MetadataStore;
import org.apache.samza.metadatastore.MetadataStoreFactory;
import org.apache.samza.metrics.MetricsRegistry;
import org.apache.samza.runtime.LocationId;
import org.apache.samza.serializers.Serde;
import org.apache.samza.util.Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Task assignment Manager is used to persist and read the task-to-container
 * assignment information from the coordinator stream
 * */
public class TaskAssignmentManager {
  private static final Logger LOG = LoggerFactory.getLogger(TaskAssignmentManager.class);
  private Map<String, String> taskNameToContainerId = new HashMap<>();
  private static final String SOURCE = "SamzaTaskAssignmentManager";
  private final Config config;
  private final Serde<String> valueSerde;
  private final Serde<String> keySerde;
  private SamzaContainerContext containerContext;
  private MetadataStore metadataStore;

  /**
   * Default constructor that creates a read-write manager
   *
   * @param config denotes the configuration required to instantiate metadata store.
   */
  public TaskAssignmentManager(Config config, MetricsRegistry metricsRegistry) {
    this(new MapConfig(config, ImmutableMap.of("coordinator.stream.message.type", SetTaskContainerMapping.TYPE)),
         metricsRegistry,
         new LocalityManager.CoordinatorStreamKeySerde(SetTaskContainerMapping.TYPE),
         new LocalityManager.CoordinatorStreamValueSerde(config, SetTaskContainerMapping.TYPE));
  }

  public TaskAssignmentManager(Config config, MetricsRegistry metricsRegistry, Serde<String> keySerde, Serde<String> valueSerde) {
    this.config = config;
    this.keySerde = keySerde;
    this.valueSerde = valueSerde;
    MetadataStoreFactory metadataStoreFactory = Util.getObj(new JobConfig(config).getMetadataStoreFactory());
    this.metadataStore = metadataStoreFactory.getMetadataStore(config, metricsRegistry);
  }

  public void init(SamzaContainerContext containerContext) {
    this.containerContext = containerContext;
    this.metadataStore.init(containerContext);
  }

  /**
   * Method to allow read container task information from coordinator stream. This method is used
   * in {@link org.apache.samza.coordinator.JobModelManager}.
   *
   * @return the map of taskName: containerId
   */
  public Map<String, String> readTaskAssignment() {
    taskNameToContainerId.clear();
    metadataStore.all().forEach((keyBytes, valueBytes) -> {
        String taskName = keySerde.fromBytes(keyBytes);
        String locationId = valueSerde.fromBytes(valueBytes);
        taskNameToContainerId.put(taskName, locationId);
        LOG.debug("Assignment for task \"{}\": {}", taskName, locationId);
      });
    return Collections.unmodifiableMap(new HashMap<>(taskNameToContainerId));
  }

  /**
   * Method to write task container info to coordinator stream.
   *
   * @param taskName    the task name
   * @param containerId the SamzaContainer ID or {@code null} to delete the mapping
   */
  public void writeTaskContainerMapping(String taskName, String containerId) {
    String existingContainerId = taskNameToContainerId.get(taskName);
    if (existingContainerId != null && !existingContainerId.equals(containerId)) {
      LOG.info("Task \"{}\" moved from container {} to container {}", new Object[]{taskName, existingContainerId, containerId});
    } else {
      LOG.debug("Task \"{}\" assigned to container {}", taskName, containerId);
    }

    if (containerId == null) {
      metadataStore.remove(keySerde.toBytes(taskName));
      taskNameToContainerId.remove(taskName);
    } else {
      metadataStore.put(keySerde.toBytes(taskName), valueSerde.toBytes(containerId));
      taskNameToContainerId.put(taskName, containerId);
    }
  }

  /**
   * Deletes the task container info from the coordinator stream for each of the specified task names.
   *
   * @param taskNames the task names for which the mapping will be deleted.
   */
  public void deleteTaskMappings(Iterator<TaskName> taskNames) {
    for (TaskName taskName : (Iterable<? extends TaskName>) (() -> taskNames)) {
      metadataStore.remove(keySerde.toBytes(taskName.getTaskName()));
    }
  }

  public Map<TaskName, LocationId> readTaskLocality() {
    Map<TaskName, LocationId> taskLocality = new HashMap<>();
    metadataStore.all().forEach((keyBytes, valueBytes) -> {
        taskLocality.put(new TaskName(keySerde.fromBytes(keyBytes)), new LocationId(valueSerde.fromBytes(valueBytes)));
        LOG.debug("Locality for task \"{}\": {}", keyBytes, valueBytes);
      });
    return taskLocality;
  }

  public void writeTaskLocality(TaskName taskName, LocationId locationId) {
    metadataStore.put(keySerde.toBytes(taskName.getTaskName()), valueSerde.toBytes(locationId.getId()));
  }

  public void close() {
    metadataStore.close();
  }
}
