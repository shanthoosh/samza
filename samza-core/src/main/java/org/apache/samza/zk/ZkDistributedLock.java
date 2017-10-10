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

import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.samza.SamzaException;
import org.apache.samza.config.Config;
import org.apache.samza.coordinator.DistributedLockWithState;
import org.apache.samza.coordinator.LockCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Distributed lock primitive for Zookeeper.
 */
public class ZkDistributedLock implements DistributedLockWithState {

  public static final Logger LOG = LoggerFactory.getLogger(ZkDistributedLock.class);
  private static final String STATE_INITED = "sate_initialized";
  private final ZkUtils zkUtils;
  private final String lockPath;
  private final String participantId;
  private final ZkKeyBuilder keyBuilder;
  private final Random random = new Random();
  private final RunIdGenerator runIdGenerator;
  private final Config config;
  private String nodePath = null;
  private final String statePath;

  public ZkDistributedLock(String participantId, ZkUtils zkUtils, String lockId, RunIdGenerator runIdGenerator, Config config) {
    this.zkUtils = zkUtils;
    this.participantId = participantId;
    this.keyBuilder = zkUtils.getKeyBuilder();
    this.runIdGenerator = runIdGenerator;
    this.config = config;
    lockPath = String.format("%s/stateLock_%s", keyBuilder.getRootPath(), lockId);
    statePath = String.format("%s/%s_%s", lockPath, STATE_INITED, lockId);
    zkUtils.validatePaths(new String[] {lockPath});
  }

  /**
   * Tries to acquire a lock in order to create intermediate streams. On failure to acquire lock, it keeps trying until the lock times out.
   * Creates a sequential ephemeral node to acquire the lock. If the path of this node has the lowest sequence number, the processor has acquired the lock.
   * @param timeout Duration of lock acquiring timeout.
   * @param unit Unit of the timeout defined above.
   * @return true if lock is acquired successfully, false if it times out.
   */
  @Override
  public LockResponse lockIfNotSet(long timeout, TimeUnit unit)
      throws TimeoutException {

    nodePath = zkUtils.getZkClient().createEphemeralSequential(lockPath + "/", null);

    //Start timer for timeout
    long startTime = System.currentTimeMillis();
    long lockTimeout = TimeUnit.MILLISECONDS.convert(timeout, unit);

    // TODO: refactor them into smaller sub methods.
    ZkNodeData zkNodeData = null;
    while ((System.currentTimeMillis() - startTime) < lockTimeout) {

      List<String> children = zkUtils.getZkClient().getChildren(lockPath);
      int index = children.indexOf(ZkKeyBuilder.parseIdFromPath(nodePath));

      if (children.size() == 0 || index == -1) {
        throw new SamzaException("Looks like we are no longer connected to Zk. Need to reconnect!");
      }
      // Acquires lock when the node has the lowest sequence number and returns.
      if (index == 0) {
        LOG.info("Acquired lock for participant id: {}", participantId);
        ZkNodeData existingZkNodeData = zkUtils.getZkClient().readData(nodePath);
        if (existingZkNodeData == null || existingZkNodeData.getRunId() == null) {
          // Generate run.id by leader.
          String runId = runIdGenerator.generateRunId(config);
          zkNodeData = new ZkNodeData(runId, participantId);
          zkUtils.writeData(nodePath, zkNodeData);
          return new LockResponse(runId, true);
        } else {
          return new LockResponse(existingZkNodeData.getRunId(), true);
        }
      } else {
        // Read run.id by leader and store it in the follower's node.
        Collections.sort(children);
        String leaderPath = children.get(0);
        ZkNodeData leaderZkNodeData = zkUtils.getZkClient().readData(leaderPath);
        if (leaderZkNodeData != null) {
          zkNodeData = new ZkNodeData(leaderZkNodeData.getRunId(), participantId);
          zkUtils.writeData(nodePath, zkNodeData);
        }
        try {
          Thread.sleep(random.nextInt(1000));
        } catch (InterruptedException e) {
          Thread.interrupted();
        }
        LOG.info("Trying to acquire lock again...");
      }
    }
    if (zkNodeData != null) {
      return new LockResponse(zkNodeData.getRunId(), false);
    } else {
      // TODO: update the exception message.
      throw new TimeoutException("could not acquire lock for " + timeout + " " + unit.toString());
    }
  }

  /**
   * Unlocks, by deleting the ephemeral sequential node created to acquire the lock.
   */
  @Override
  public void unlockAndSet() {
    // set state
    zkUtils.getZkClient().createPersistent(statePath, true);

    if (nodePath != null) {
      zkUtils.getZkClient().delete(nodePath);
      nodePath = null;
      LOG.info("Ephemeral lock node deleted. Unlocked!");
    } else {
      LOG.warn("Ephemeral lock node you want to delete doesn't exist");
    }
  }

  private static class ZkNodeData {
    String runId;
    String participantId;

    ZkNodeData(String runId, String participantId) {
      this.runId = runId;
      this.participantId = participantId;
    }

    String getRunId() {
      return runId;
    }

    String getParticipantId() {
      return participantId;
    }
  }
}