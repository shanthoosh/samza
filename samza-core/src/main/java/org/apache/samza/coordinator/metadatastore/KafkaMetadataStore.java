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

import org.apache.samza.config.Config;
import org.apache.samza.container.LocalityManager;
import org.apache.samza.container.SamzaContainerContext;
import org.apache.samza.container.TaskName;
import org.apache.samza.container.grouper.task.TaskAssignmentManager;
import org.apache.samza.coordinator.stream.CoordinatorStreamManager;
import org.apache.samza.coordinator.stream.messages.SetContainerHostMapping;
import org.apache.samza.metadatastore.MetadataStore;
import org.apache.samza.metrics.MetricsRegistry;
import org.apache.samza.runtime.LocationId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * An implementation of the {@link MetadataStore} interface where the
 * locality of the Samza job is stored in coordinator stream.
 */
public class KafkaMetadataStore implements MetadataStore {

  private static final Logger LOG = LoggerFactory.getLogger(KafkaMetadataStore.class);
  private final Config config;
  private final CoordinatorStreamManager coordinatorStreamManager;
  private final LocalityManager localityManager;
  private SamzaContainerContext containerContext;

  public KafkaMetadataStore(Config config, MetricsRegistry metricsRegistry) {
    this.config = config;
    this.coordinatorStreamManager = new CoordinatorStreamManager(config, metricsRegistry);
    this.localityManager = new LocalityManager(coordinatorStreamManager);
  }

  KafkaMetadataStore(Config config, CoordinatorStreamManager coordinatorStreamManager, LocalityManager localityManager) {
    this.config = config;
    this.coordinatorStreamManager = coordinatorStreamManager;
    this.localityManager = localityManager;
  }

  @Override
  public void init(SamzaContainerContext containerContext) {
    LOG.info("Starting the coordinator stream system consumer with config: {}.", config);
    this.containerContext = containerContext;
    coordinatorStreamManager.start();
    String containerName = String.format("SamzaContainer-%s", this.containerContext.id);
    coordinatorStreamManager.register(containerName);
  }

  @Override
  public Map<TaskName, LocationId> readTaskLocality() {
    TaskAssignmentManager taskAssignmentManager = localityManager.getTaskAssignmentManager();
    Map<String, String> taskNameToContainerId = taskAssignmentManager.readTaskAssignment();
    Map<String, LocationId> containerToLocationId = readProcessorLocality();
    Map<TaskName, LocationId> taskNameToLocationId = new HashMap<>();
    taskNameToContainerId.forEach((taskName, containerId) -> {
        LocationId locationId = containerToLocationId.get(containerId);
        taskNameToLocationId.put(new TaskName(taskName), locationId);
      });
    return taskNameToLocationId;
  }

  @Override
  public Map<String, LocationId> readProcessorLocality() {
    Map<String, Map<String, String>> containerToLocalityInfo = localityManager.readContainerLocality();
    Map<String, LocationId> processorLocality = new HashMap<>();
    for (Map.Entry<String, Map<String, String>> entry : containerToLocalityInfo.entrySet()) {
      String processorId = entry.getKey();
      String locationId = entry.getValue().get(SetContainerHostMapping.HOST_KEY);
      processorLocality.put(processorId, new LocationId(locationId));
    }
    LOG.info("Read the locality info: {} from coordinator stream.", processorLocality);
    return processorLocality;
  }

  @Override
  public void writeTaskLocality(Collection<TaskName> taskNames, LocationId locationId) {
    LOG.info("Writing container to host mapping for container: {}, locationId: {}.", containerContext.id, locationId.getId());
    localityManager.writeContainerToHostMapping(containerContext.id, locationId.getId(), "", "");
  }

  @Override
  public void deleteTaskLocality(Collection<TaskName> tasks) {
    TaskAssignmentManager taskAssignmentManager = localityManager.getTaskAssignmentManager();
    Set<String> taskNames = new HashSet<>();
    for (TaskName taskName : tasks) {
      taskNames.add(taskName.getTaskName());
    }
    taskAssignmentManager.deleteTaskContainerMappings(taskNames);
  }

  @Override
  public void close() {
    LOG.info("Stopping the coordinator stream system consumer.", config);
    coordinatorStreamManager.stop();
  }
}
