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

import com.google.common.collect.ImmutableSet;
import com.google.common.io.Files;
import java.io.File;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;
import org.apache.samza.Partition;
import org.apache.samza.config.MapConfig;
import org.apache.samza.container.TaskName;
import org.apache.samza.job.model.ContainerModel;
import org.apache.samza.job.model.JobModel;
import org.apache.samza.job.model.TaskModel;
import org.apache.samza.system.SystemStreamPartition;

public class JobModelCompressionComparator {

  public static void main(String[] args) throws Exception {
    compressJobModelUsingGzipCompression();
    compressJobModelUsingSnappyCompression();
  }

  private static void compressJobModelUsingSnappyCompression() throws Exception {
    File file = new File("/tmp/jobmodel-snappy-compression.txt");
    for (int partitionCountPerContainer = 100; partitionCountPerContainer <= 20000; partitionCountPerContainer += 100) {
      JobModel jobModel = generateTestJobModel(partitionCountPerContainer, 1);
      byte[] jobModelAsBytes = JobModelCompressionUtil.convertJobModelToBytes(jobModel);
      long uncompressedJobModelSize = jobModelAsBytes.length;
      long compressedJobModelSize = JobModelCompressionUtil.getSnappyCompressedObjectSize(jobModel);
      System.out.println(partitionCountPerContainer + " " + uncompressedJobModelSize + " " + compressedJobModelSize);
      Files.append(String.format("%d %d %d\n", partitionCountPerContainer, uncompressedJobModelSize, compressedJobModelSize), file, Charset.defaultCharset());
    }
  }

  private static void compressJobModelUsingGzipCompression() throws Exception {
    File file = new File("/tmp/jobmodel-gzip-compression.txt");
    for (int partitionCountPerContainer = 100; partitionCountPerContainer <= 20000; partitionCountPerContainer += 100) {
      JobModel jobModel = generateTestJobModel(partitionCountPerContainer, 1);
      byte[] jobModelAsBytes = JobModelCompressionUtil.convertJobModelToBytes(jobModel);
      long uncompressedJobModelSize = jobModelAsBytes.length;
      long compressedJobModelSize = JobModelCompressionUtil.getGzipCompressedJobModelSize(jobModel);
      System.out.println(partitionCountPerContainer + " " + uncompressedJobModelSize + " " + compressedJobModelSize);
      Files.append(String.format("%d %d %d\n", partitionCountPerContainer, uncompressedJobModelSize, compressedJobModelSize), file, Charset.defaultCharset());
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
