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
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
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

  /**
   * Add more java docs.
   */
  private static final long TASK_WAIT_TIMEOUT_MS = TimeUnit.SECONDS.toMillis(10);

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

  /**
   * Fill in java doc.
   */
  private final ScheduledTaskThreadPool scheduledTaskThreadPool;

  /**
   * Fill in java doc.
   */
  private final Map<String, ScheduledFuture> futureHandles;

  // Ideally, this should be only used for testing. But ZkBarrierForVersionUpgrades uses it. This needs to be fixed.
  // TODO: Timer shouldn't be passed around the components. It should be associated with the JC or the caller of
  // coordinationUtils.
  public ScheduleAfterDebounceTime() {
    this(null);
  }

  public ScheduleAfterDebounceTime(ScheduledTaskFailureCallback scheduledTaskFailureCallback) {
    ThreadFactory threadFactory = new ThreadFactoryBuilder().setNameFormat("debounce-thread-%d").setDaemon(true).build();
    scheduledTaskThreadPool = new ScheduledTaskThreadPool(1, threadFactory, scheduledTaskFailureCallback);
    futureHandles = new HashMap<>();
  }

  synchronized public void scheduleAfterDebounceTime(String actionName, long debounceTimeMs, Runnable runnable) {
    // check if this action has been scheduled already
    ScheduledFuture sf = futureHandles.get(actionName);
    if (sf != null && !sf.isDone()) {
      LOG.info("cancel future for " + actionName);
      // attempt to cancel
      if (!sf.cancel(false)) {
      }
      futureHandles.remove(actionName);
    }
    // schedule a new task
    sf = scheduledTaskThreadPool.schedule(actionName, runnable, debounceTimeMs, TimeUnit.MILLISECONDS);
    futureHandles.put(actionName, sf);
  }

  public void stopScheduler() {
    /**
     * Add javadoc here.
     */
    scheduledTaskThreadPool.shutdownNow();
  }

  /**
   * Purpose : To receive interrupted exception when using ScheduledExecutorService.
   * Fill in java doc.
   */
  private class ScheduledTaskThreadPool extends ScheduledThreadPoolExecutor {

    /**
     * Fill in java doc.
     */
    private final Optional<ScheduledTaskFailureCallback> taskFailureCallbackOptional;

    /**
     * Fill in java doc.
     */
    private final Map<Runnable, String> submittedTasks;

    public ScheduledTaskThreadPool(int corePoolSize, ThreadFactory threadFactory, ScheduledTaskFailureCallback taskFailureCallback) {
      super(corePoolSize, threadFactory);
      this.taskFailureCallbackOptional = Optional.ofNullable(taskFailureCallback);
      submittedTasks = new HashMap<>();
    }

    /**
     *
     * @param actionName
     * @param command
     * @param delay
     * @param unit
     * @return
     */
    public ScheduledFuture<?> schedule(String actionName, Runnable command, long delay, TimeUnit unit) {
      LOG.info("scheduled " + actionName + " in " + delay + " milliseconds");
      submittedTasks.put(command, actionName);
      return super.schedule(command, delay, unit);
    }

    @Override
    protected void afterExecute(Runnable runnable, Throwable throwable) {
      System.out.println(throwable);
      String actionName = submittedTasks.get(runnable);
      if (throwable != null) {
        /**
         * Add java doc explaining the logical sequence of actions..
         */
        // 1. Clear future handles. Since this starts a shutdown.
        futureHandles.clear();
        // 2. Report exception if taskFailureCallbackOptional is present and Log.
        taskFailureCallbackOptional.ifPresent(x -> x.onError(throwable));
        if (actionName != null) {
          LOG.error(actionName + " threw an exception.", throwable);
        }
      } else if (actionName != null) {
        LOG.debug(actionName + " completed successfully.");
      }
      super.afterExecute(runnable, throwable);
    }
  }

  interface ScheduledTaskFailureCallback {
    void onError(Throwable throwable);
  }
}
