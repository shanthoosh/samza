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

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class allows scheduling a Runnable actions after some debounce time.
 * When the same action is scheduled it needs to cancel the previous one. To accomplish that we keep the previous
 * future in a map, keyed by the action name. Here we predefine some actions, which are used in the
 * ZK based standalone app.
 */
public class ScheduleAfterDebounceTime {
  public static final Logger LOG = LoggerFactory.getLogger(ScheduleAfterDebounceTime.class);
  public static final long TIMEOUT_MS = 1000 * 10; // timeout to wait for a task to complete

  // Here we predefine some actions which are used in the ZK based standalone app.
  // Action name when the JobModel version changes
  public static final String JOB_MODEL_VERSION_CHANGE = "JobModelVersionChange";

  // Action name when the Processor membership changes
  public static final String ON_PROCESSOR_CHANGE = "OnProcessorChange";

  /**
   *
   * cleanup process is started after every new job model generation is complete.
   * It deletes old versions of job model and the barrier.
   * How many to delete (or to leave) is controlled by @see org.apache.samza.zk.ZkJobCoordinator#NUM_VERSIONS_TO_LEAVE.
   **/
  public static final String ON_ZK_CLEANUP = "OnCleanUp";

  private final Optional<ScheduledTaskCallback> scheduledTaskCallback;

  private final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(
      new ThreadFactoryBuilder().setNameFormat("debounce-thread-%d").setDaemon(true).build());
  private final Map<String, ScheduledFuture> futureHandles = new HashMap<>();

  // Ideally, this should be only used for testing. But ZkBarrierForVersionUpgrades uses it. This needs to be fixed.
  // TODO: Timer shouldn't be passed around the components. It should be associated with the JC or the caller of
  // coordinationUtils.
  public ScheduleAfterDebounceTime() { this(null); }

  public ScheduleAfterDebounceTime(ScheduledTaskCallback scheduledTaskCallback) {
    this.scheduledTaskCallback = Optional.ofNullable(scheduledTaskCallback);
  }

  synchronized public void scheduleAfterDebounceTime(String actionName, long debounceTimeMs, Runnable runnable) {
    // check if this action has been scheduled already
    ScheduledFuture sf = futureHandles.get(actionName);
    if (sf != null && !sf.isDone()) {
      LOG.info("cancel future for " + actionName);
      // attempt to cancel
      if (!sf.cancel(false)) {
        try {
          sf.get(TIMEOUT_MS, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
          // we ignore the exception
          LOG.warn("Cancel for action: {} failed.", new Object[]{actionName, e});
        }
      }
      futureHandles.remove(actionName);
    }
    // schedule a new task
    sf = scheduledExecutorService.schedule(() -> {
        try {
          runnable.run();
          LOG.debug("Action: {} completed successfully.", actionName);
          /**
           * Expects all run() implementations <b>not to swallow the interrupts.</b>
           * Interrupts are provided in following two scenarios
           * A) From the ExecutorService, asking the task to shutdown.
           * B)
           */
          if (Thread.currentThread().isInterrupted()) {
            LOG.info("Interrupt received from executor service to shutdown.");
            scheduledTaskCallback.ifPresent(callback -> callback.onError(new InterruptedException()));
          }
        } catch (Throwable throwable) {
          LOG.error("Execution of action: {} failed.", new Object[]{actionName, throwable});
          scheduledTaskCallback.ifPresent(callback -> callback.onError(throwable));
        }
      },
     debounceTimeMs,
     TimeUnit.MILLISECONDS);
    LOG.info("Scheduled action: {} to run after: {} milliseconds.", actionName, debounceTimeMs);
    futureHandles.put(actionName, sf);
  }

  private void onError(Throwable throwable) {
    // FIX IT before review: This clear action is happening from ExecutorService thread
    // and update happens in user thread. Synchronize both of them.
    // 1. Cleanup state.
    futureHandles.clear();

    // 2. ScheduledTaskCallback.onError if present.
    scheduledTaskCallback.ifPresent(callback -> callback.onError(throwable));
  }

  public void stopScheduler() {
    // shutdown executor service
    scheduledExecutorService.shutdownNow();
  }

  interface ScheduledTaskCallback {
    void onError(Throwable throwable);
  }
}
