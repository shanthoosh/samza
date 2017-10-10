package org.apache.samza.coordinator;

/**
 * Add class documentation.
 */
public interface LockCallback {
  /**
   * Add documentation.
   * @param runId
   */
  void onLockAcquired(String runId);
}
