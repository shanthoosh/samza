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

import java.util.*;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.collections4.map.DefaultedMap;
import org.apache.samza.SamzaException;
import org.apache.samza.container.LocalityManager;
import org.apache.samza.container.TaskName;
import org.apache.samza.job.model.ContainerModel;
import org.apache.samza.job.model.TaskModel;
import org.apache.samza.runtime.LocationId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Group the SSP taskNames by dividing the number of taskNames into the number
 * of containers (n) and assigning n taskNames to each container as returned by
 * iterating over the keys in the map of taskNames (whatever that ordering
 * happens to be). No consideration is given towards locality, even distribution
 * of aggregate SSPs within a container, even distribution of the number of
 * taskNames between containers, etc.
 *
 * TODO: SAMZA-1197 - need to modify balance to work with processorId strings
 */
public class GroupByContainerCount implements BalancingTaskNameGrouper {
  private static final Logger LOG = LoggerFactory.getLogger(GroupByContainerCount.class);
  private final int containerCount;

  /**
   * @param taskModels
   * @param processorLocality
   * @return
   */
  @Override
  public Set<ContainerModel> group(Set<TaskModel> taskModels, Map<String, LocationId> processorLocality) {
    Preconditions.checkArgument(taskModels.isEmpty(), "No tasks found. Likely due to no input partitions. Can't run a job with no tasks.");
    Preconditions.checkArgument(taskModels.size() < containerCount, String.format("Configured container count: %d is larger than your task count: %d. Can't have containers with nothing to do, so aborting.", containerCount, taskModels.size()));

    if (MapUtils.isEmpty(processorLocality) || processorLocality.size() == 1) {
      LOG.info("ProcessorLocality has size: {}. Generating with the default group method.", processorLocality.size());
      return group(taskModels);
    }

    Map<LocationId, List<String>> locationIdToProcessors = new DefaultedMap<>(new ArrayList<>());
    Map<String, TaskGroup> processorIdToTaskGroup = new HashMap<>();

    processorLocality.forEach((processorId, locationId) -> {
        locationIdToProcessors.get(locationId).add(processorId);
        processorIdToTaskGroup.put(processorId, new TaskGroup(processorId, new ArrayList<>()));
      });

    int numTasksPerProcessor = taskModels.size() / processorLocality.size();
    Set<TaskModel> assignedTasks = new HashSet<>();
    for (TaskModel taskModel : taskModels) {
//      List<String> processorIds = locationIdToProcessors.computeIfAbsent(taskModel.getLocationId(), k -> new ArrayList<>());
      List<String> processorIds = locationIdToProcessors.computeIfAbsent(new LocationId("0"), k -> new ArrayList<>());
      for (String processorId : processorIds) {
        TaskGroup taskGroup = processorIdToTaskGroup.get(processorId);
        if (taskGroup.size() < numTasksPerProcessor) {
          taskGroup.addTaskName(taskModel.getTaskName().getTaskName());
          assignedTasks.add(taskModel);
        }
      }
    }

    Iterator<String> processorIdsCyclicIterator = Iterators.cycle(processorLocality.keySet());
    Collection<TaskGroup> taskGroups = processorIdToTaskGroup.values();
    for (TaskModel taskModel : taskModels) {
      if (!assignedTasks.contains(taskModel)) {
        Optional<TaskGroup> underAssignedTaskGroup = taskGroups.stream()
                                                               .filter(taskGroup -> taskGroup.size() < numTasksPerProcessor)
                                                               .findFirst();
        if (underAssignedTaskGroup.isPresent()) {
          underAssignedTaskGroup.get().addTaskName(taskModel.getTaskName().getTaskName());
        } else {
          TaskGroup taskGroup = processorIdToTaskGroup.get(processorIdsCyclicIterator.next());
          taskGroup.addTaskName(taskModel.getTaskName().getTaskName());
        }
      }
    }

    return buildContainerModels(taskModels, taskGroups);
  }

  public GroupByContainerCount(int containerCount) {
    if (containerCount <= 0) throw new IllegalArgumentException("Must have at least one container");
    this.containerCount = containerCount;
  }

  @Override
  public Set<ContainerModel> group(Set<TaskModel> tasks) {

    validateTasks(tasks);

    // Sort tasks by taskName.
    List<TaskModel> sortedTasks = new ArrayList<>(tasks);
    Collections.sort(sortedTasks);

    // Map every task to a container in round-robin fashion.
    Map<TaskName, TaskModel>[] taskGroups = new Map[containerCount];
    for (int i = 0; i < containerCount; i++) {
      taskGroups[i] = new HashMap<>();
    }
    for (int i = 0; i < sortedTasks.size(); i++) {
      TaskModel tm = sortedTasks.get(i);
      taskGroups[i % containerCount].put(tm.getTaskName(), tm);
    }

    // Convert to a Set of ContainerModel
    Set<ContainerModel> containerModels = new HashSet<>();
    for (int i = 0; i < containerCount; i++) {
      containerModels.add(new ContainerModel(String.valueOf(i), i, taskGroups[i]));
    }

    return Collections.unmodifiableSet(containerModels);
  }

  @Override
  public Set<ContainerModel> balance(Set<TaskModel> tasks, LocalityManager localityManager) {
    validateTasks(tasks);

    if (localityManager == null) {
      LOG.info("Locality manager is null. Cannot read or write task assignments. Invoking grouper.");
      return group(tasks);
    }

    TaskAssignmentManager taskAssignmentManager = localityManager.getTaskAssignmentManager();
    List<TaskGroup> containers = getPreviousContainers(taskAssignmentManager, tasks.size());
    if (containers == null || containers.size() == 1 || containerCount == 1) {
      LOG.info("Balancing does not apply. Invoking grouper.");
      return group(tasks);
    }

    int prevContainerCount = containers.size();
    int containerDelta = containerCount - prevContainerCount;
    if (containerDelta == 0) {
      LOG.info("Container count has not changed. Reusing previous container models.");
      return buildContainerModels(tasks, containers);
    }
    LOG.info("Container count changed from {} to {}. Balancing tasks.", prevContainerCount, containerCount);

    // Calculate the expected task count per container
    int[] expectedTaskCountPerContainer = calculateTaskCountPerContainer(tasks.size(), prevContainerCount, containerCount);

    // Collect excess tasks from over-assigned containers
    List<String> taskNamesToReassign = new LinkedList<>();
    for (int i = 0; i < prevContainerCount; i++) {
      TaskGroup taskGroup = containers.get(i);
      while (taskGroup.size() > expectedTaskCountPerContainer[i]) {
        taskNamesToReassign.add(taskGroup.removeTask());
      }
    }

    // Assign tasks to the under-assigned containers
    if (containerDelta > 0) {
      List<TaskGroup> newContainers = createContainers(prevContainerCount, containerCount);
      containers.addAll(newContainers);
    } else {
      containers = containers.subList(0, containerCount);
    }
    assignTasksToContainers(expectedTaskCountPerContainer, taskNamesToReassign, containers);

    // Transform containers to containerModel
    return buildContainerModels(tasks, containers);
  }

  /**
   * Reads the task-container mapping from the provided {@link TaskAssignmentManager} and returns a
   * list of TaskGroups, ordered ascending by containerId.
   *
   * @param taskAssignmentManager the {@link TaskAssignmentManager} that will be used to retrieve the previous mapping.
   * @param taskCount             the number of tasks, for validation against the persisted tasks.
   * @return                      a list of TaskGroups, ordered ascending by containerId or {@code null}
   *                              if the previous mapping doesn't exist or isn't usable.
   */
  private List<TaskGroup> getPreviousContainers(TaskAssignmentManager taskAssignmentManager, int taskCount) {
    Map<String, String> taskToContainerId = taskAssignmentManager.readTaskAssignment();
    taskToContainerId.values().forEach(id -> {
        try {
          int intId = Integer.parseInt(id);
        } catch (NumberFormatException nfe) {
          throw new SamzaException("GroupByContainerCount cannot handle non-integer processorIds!", nfe);
        }
      });
    if (taskToContainerId.isEmpty()) {
      LOG.info("No task assignment map was saved.");
      return null;
    } else if (taskCount != taskToContainerId.size()) {
      return null;
    }

    List<TaskGroup> containers;
    try {
      containers = getOrderedContainers(taskToContainerId);
    } catch (Exception e) {
      LOG.error("Exception while parsing task mapping", e);
      return null;
    }
    return containers;
  }

  /**
   * Verifies the input tasks argument and throws {@link IllegalArgumentException} if it is invalid.
   *
   * @param tasks the tasks to validate.
   */
  private void validateTasks(Set<TaskModel> tasks) {
    if (tasks.size() <= 0)
      throw new IllegalArgumentException("No tasks found. Likely due to no input partitions. Can't run a job with no tasks.");

    if (tasks.size() < containerCount)
      throw new IllegalArgumentException(String.format(
          "Your container count (%s) is larger than your task count (%s). Can't have containers with nothing to do, so aborting.",
          containerCount,
          tasks.size()));
  }

  /**
   * Creates a list of empty {@link TaskGroup} instances for a range of container id's
   * from the start(inclusive) to end(exclusive) container id.
   *
   * @param startContainerId  the first container id for which a TaskGroup is needed.
   * @param endContainerId    the first container id AFTER the last TaskGroup that is needed.
   * @return                  a set of empty TaskGroup instances corresponding to the range
   *                          [startContainerId, endContainerId)
   */
  private List<TaskGroup> createContainers(int startContainerId, int endContainerId) {
    List<TaskGroup> containers = new ArrayList<>(endContainerId - startContainerId);
    for (int i = startContainerId; i < endContainerId; i++) {
      TaskGroup taskGroup = new TaskGroup(String.valueOf(i), new ArrayList<String>());
      containers.add(taskGroup);
    }
    return containers;
  }

  /**
   * Assigns tasks from the specified list to containers that have fewer containers than indicated
   * in taskCountPerContainer.
   *
   * @param taskCountPerContainer the expected number of tasks for each container.
   * @param taskNamesToAssign     the list of tasks to assign to the containers.
   * @param containers            the containers (as {@link TaskGroup}) to which the tasks will be assigned.
   */
  // TODO: Change logic from using int arrays to a Map<String, Integer> (id -> taskCount)
  private void assignTasksToContainers(int[] taskCountPerContainer, List<String> taskNamesToAssign,
      List<TaskGroup> containers) {
    for (TaskGroup taskGroup : containers) {
      for (int j = taskGroup.size(); j < taskCountPerContainer[Integer.valueOf(taskGroup.getContainerId())]; j++) {
        String taskName = taskNamesToAssign.remove(0);
        taskGroup.addTaskName(taskName);
        LOG.info("Assigned task {} to container {}", taskName, taskGroup.getContainerId());
      }
    }
  }

  /**
   * Calculates the expected number of tasks for each container. The count is generated for
   * max(oldContainerCount, newContainerCount) s.t. if the container count has decreased,
   * the excess containers will have a count == 0, indicating that any tasks assigned to
   * them should be reassigned.
   *
   * @param taskCount             the number of tasks to divide among the containers.
   * @param prevContainerCount    the previous number of containers.
   * @param currentContainerCount the current number of containers.
   * @return                      the expected number of tasks for each container.
   */
  private int[] calculateTaskCountPerContainer(int taskCount, int prevContainerCount, int currentContainerCount) {
    int[] newTaskCountPerContainer = new int[Math.max(currentContainerCount, prevContainerCount)];
    Arrays.fill(newTaskCountPerContainer, 0);

    for (int i = 0; i < currentContainerCount; i++) {
      newTaskCountPerContainer[i] = taskCount / currentContainerCount;
      if (taskCount % currentContainerCount > i) {
        newTaskCountPerContainer[i]++;
      }
    }
    return newTaskCountPerContainer;
  }

  /**
   * Translates the list of TaskGroup instances to a set of ContainerModel instances, using the
   * set of TaskModel instances.
   *
   * @param tasks             the TaskModels to assign to the ContainerModels.
   * @param containerTasks    the TaskGroups defining how the tasks should be grouped.
   * @return                  a mutable set of ContainerModels.
   */
  private Set<ContainerModel> buildContainerModels(Set<TaskModel> tasks, Collection<TaskGroup> containerTasks) {
    // Map task names to models
    Map<String, TaskModel> taskNameToModel = new HashMap<>();
    for (TaskModel model : tasks) {
      taskNameToModel.put(model.getTaskName().getTaskName(), model);
    }

    // Build container models
    Set<ContainerModel> containerModels = new HashSet<>();
    for (TaskGroup container : containerTasks) {
      Map<TaskName, TaskModel> containerTaskModels = new HashMap<>();
      for (String taskName : container.taskNames) {
        TaskModel model = taskNameToModel.get(taskName);
        containerTaskModels.put(model.getTaskName(), model);
      }
      containerModels.add(
          new ContainerModel(container.containerId, Integer.valueOf(container.containerId), containerTaskModels));
    }
    return Collections.unmodifiableSet(containerModels);
  }

  /**
   * Converts the task->containerId map to an ordered list of {@link TaskGroup} instances.
   *
   * @param taskToContainerId a map from each task name to the containerId to which it is assigned.
   * @return                  a list of TaskGroups ordered ascending by containerId.
   */
  private List<TaskGroup> getOrderedContainers(Map<String, String> taskToContainerId) {
    LOG.debug("Got task to container map: {}", taskToContainerId);

    // Group tasks by container Id
    Map<String, List<String>> containerIdToTaskNames = new HashMap<>();
    for (Map.Entry<String, String> entry : taskToContainerId.entrySet()) {
      String taskName = entry.getKey();
      String containerId = entry.getValue();
      List<String> taskNames = containerIdToTaskNames.computeIfAbsent(containerId, k -> new ArrayList<>());
      taskNames.add(taskName);
    }

    // Build container tasks
    List<TaskGroup> containerTasks = new ArrayList<>(containerIdToTaskNames.size());
    for (int i = 0; i < containerIdToTaskNames.size(); i++) {
      if (containerIdToTaskNames.get(String.valueOf(i)) == null) throw new IllegalStateException("Task mapping is missing container: " + i);
      containerTasks.add(new TaskGroup(String.valueOf(i), containerIdToTaskNames.get(String.valueOf(i))));
    }

    return containerTasks;
  }

  /**
   * A mutable group of tasks and an associated container id.
   *
   * Used as a temporary mutable container until the final ContainerModel is known.
   */
  private static class TaskGroup {
    private final List<String> taskNames = new LinkedList<>();
    private final String containerId;

    private TaskGroup(String containerId, List<String> taskNames) {
      this.containerId = containerId;
      Collections.sort(taskNames); // For consistency because the taskNames came from a Map
      this.taskNames.addAll(taskNames);
    }

    public String getContainerId() {
      return containerId;
    }

    public void addTaskName(String taskName) {
      taskNames.add(taskName);
    }

    public String removeTask() {
      return taskNames.remove(taskNames.size() - 1);
    }

    public int size() {
      return taskNames.size();
    }
  }
}
