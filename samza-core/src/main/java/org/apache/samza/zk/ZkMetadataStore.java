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

import org.I0Itec.zkclient.ZkClient;
import org.apache.samza.SamzaException;
import org.apache.samza.config.Config;
import org.apache.samza.config.ZkConfig;
import org.apache.samza.container.SamzaContainerContext;
import org.apache.samza.metrics.MetricsRegistry;
import org.apache.samza.metadatastore.MetadataStore;

import java.util.Arrays;
import java.util.Map;

/**
 * An implementation of the {@link MetadataStore} interface where the
 * metadata of the Samza job is stored in zookeeper.
 *
 * LocationId for each task is stored in an persistent node in zookeeper.
 * Task locality is stored in the following format in zookeeper:
 *
 * - {zkBaseRootPath/$appName-$appId-$JobName-$JobId-$stageId/}
 *    - localityData
 *        - task01/
 *           locationId1 (stored as value in the task zookeeper node)
 *        - task02/
 *           locationId2 (stored as value in the task zookeeper node)
 *        ...
 *        - task0N/
 *           locationIdN (stored as value in the task zookeeper node)
 *
 * LocationId for each processor is stored in an ephemeral node in zookeeper.
 * Processor locality is stored in the following format in zookeeper:
 *
 * - {zkBaseRootPath/$appName-$appId-$JobName-$JobId-$stageId/}
 *    - processors/
 *        - processor.000001/
 *            locatoinId1 (stored as value in processor zookeeper node)
 *        - processor.000002/
 *            locationId2 (stored as value in processor zookeeper node)
 *
 */
public class ZkMetadataStore implements MetadataStore {

  private final Config config;

  private final MetricsRegistry metricsRegistry;

  private final ZkUtils zkUtils;

  private final String storeBasePath;

  public ZkMetadataStore(Config config, MetricsRegistry metricsRegistry) {
    this.config = config;
    this.metricsRegistry = metricsRegistry;
    this.zkUtils = getZkUtils(this.config, this.metricsRegistry);
    this.storeBasePath = zkUtils.getKeyBuilder().getTaskLocalityPath();
  }

  private static ZkUtils getZkUtils(Config config, MetricsRegistry metricsRegistry) {
    ZkConfig zkConfig = new ZkConfig(config);
    try {
      ZkKeyBuilder keyBuilder = new ZkKeyBuilder(zkConfig.getZkCoordinatorBasePath());
      ZkClient zkClient = new ZkClient(zkConfig.getZkConnect(), zkConfig.getZkSessionTimeoutMs(), zkConfig.getZkConnectionTimeoutMs());
      return new ZkUtils(keyBuilder, zkClient, zkConfig.getZkConnectionTimeoutMs(), metricsRegistry);
    } catch (Exception e) {
      // Handling exceptions thrown by ZkClient during instantiation.
      throw new SamzaException(String.format("Exception occurred when establishing zkClient connection at: %s." , zkConfig.getZkConnect()), e);
    }
  }

  @Override
  public void init(SamzaContainerContext containerContext) {
    zkUtils.connect();
  }

  @Override
  public byte[] get(byte[] key) {
    return zkUtils.getZkClient().readData(zkUtils.getKeyBuilder().getTaskLocalityPath());
  }

  @Override
  public void put(byte[] key, byte[] value) {
    zkUtils.writeData(storeBasePath + Arrays.toString(key), value);
  }

  @Override
  public void remove(byte[] key) {
    zkUtils.getZkClient().delete(storeBasePath + Arrays.toString(key));
  }

  @Override
  public Map<byte[], byte[]> all() {
    return null;
  }

  @Override
  public void close() {
    zkUtils.close();
  }
}
