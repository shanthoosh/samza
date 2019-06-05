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
package org.apache.samza.zk;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.Files;
import java.io.File;
import java.io.IOException;
import java.lang.instrument.Instrumentation;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.samza.Partition;
import org.apache.samza.config.MapConfig;
import org.apache.samza.container.TaskName;
import org.apache.samza.job.model.ContainerModel;
import org.apache.samza.job.model.JobModel;
import org.apache.samza.job.model.TaskModel;
import org.apache.samza.metrics.Counter;
import org.apache.samza.metrics.MetricsBase;
import org.apache.samza.metrics.MetricsRegistry;
import org.apache.samza.system.SystemStreamPartition;


/**
 * Contains all the metrics published by {@link ZkUtils}.
 */
public class ZkUtilsMetrics extends MetricsBase {
  /**
   * Number of data reads from zookeeper.
   */
  public final Counter reads;

  /**
   * Number of data writes into zookeeper.
   */
  public final Counter writes;

  /**
   * Number of subscriptions created with zookeeper.
   */
  public final Counter subscriptions;

  /**
   * Number of zookeeper connection errors in ZkClient.
   */
  public final Counter zkConnectionError;

  /**
   * Number of zookeeper data node deletions.
   */
  public final Counter deletions;

  public ZkUtilsMetrics(MetricsRegistry metricsRegistry) {
    super(metricsRegistry);
    this.reads = newCounter("reads");
    this.writes = newCounter("writes");
    this.deletions = newCounter("deletions");
    this.subscriptions = newCounter("subscriptions");
    this.zkConnectionError = newCounter("zk-connection-errors");
  }

  public static void main(String[] args) throws IOException {
    Map<String, String> configMap = new HashMap<>();
//    configMap.put("job.coordinator.zk.connect", "zk-ltx1-shared.stg.linkedin.com:12913");

    File file = new File("/tmp/latency.txt");
    configMap.put("job.coordinator.zk.connect", "localhost:2181");
    MapConfig config = new MapConfig(configMap);
    for (int partitionCountPerContainer = 100; partitionCountPerContainer <= 1500; partitionCountPerContainer += 100) {
      List<Long> latencyList = new ArrayList<>();
      long totalTestRuns = 10;
      long objectSize = 0;
      for (int testRun = 1; testRun <= totalTestRuns; ++testRun) {
        long startTime = System.currentTimeMillis();
        JobModel jobModel = generateTestJobModel(partitionCountPerContainer, 10);
        System.out.println("Generated the JobModel. Writing it to zookeeper");
        ZkUtils.storeJobModel(jobModel, config, String.valueOf(testRun));

        long endTime = System.currentTimeMillis();
        objectSize = ObjectSizeFetcher.getObjectSize(jobModel);
        latencyList.add((endTime - startTime + 1));
      }
      long totalLatencySum = 0;
      for (Long latency : latencyList) {
        totalLatencySum += latency;
      }
      System.out.println("PartitionCount: " + partitionCountPerContainer + " Object size: " + objectSize + " Avg time: " + (totalLatencySum) / (totalTestRuns * 1000) + " seconds");

      Files.append("PartitionCount: " + partitionCountPerContainer + " Object size: " + objectSize + " Avg time: " + (totalLatencySum) / (totalTestRuns * 1000) + " seconds\n", file, Charset.defaultCharset());
    }
  }

  static JobModel generateTestJobModel(int partitionPerContainer, int containerCount) {
    Map<String, ContainerModel> containerModelMap = new HashMap<>();
    int globalPartitionCounter = 0;
    for (int container = 1; container <= containerCount; ++container) {
      Map<TaskName, TaskModel> taskModelMap = new HashMap<>();
      for (int partition=1; partition <= partitionPerContainer; ++partition) {
        TaskName taskName = new TaskName("Task " + (partition + globalPartitionCounter));
        SystemStreamPartition systemStreamPartition = new SystemStreamPartition("test-system", "test-stream", new Partition(partition + globalPartitionCounter));
        taskModelMap.put(taskName, new TaskModel(taskName, ImmutableSet.of(systemStreamPartition), new Partition(partition + globalPartitionCounter)));
      }
      globalPartitionCounter += partitionPerContainer;
      containerModelMap.put(String.valueOf(container), new ContainerModel(String.valueOf(container), taskModelMap));
    }
    return new JobModel(new MapConfig(), containerModelMap);
  }
}
