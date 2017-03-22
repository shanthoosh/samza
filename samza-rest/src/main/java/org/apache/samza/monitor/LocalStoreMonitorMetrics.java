package org.apache.samza.monitor;

import org.apache.samza.metrics.Counter;
import org.apache.samza.metrics.MetricsBase;
import org.apache.samza.metrics.MetricsRegistry;

/**
 * Contains all the metrics published by {@link LocalStoreMonitor}.
 */
public class LocalStoreMonitorMetrics extends MetricsBase {

  /** Total number of task partition stores deleted by the LocalStoreMonitor.  */
  public final Counter noOfDeletedTaskPartitionStores;

  /* Total disk space cleared by the LocalStoreMonitor. */
  public final Counter diskSpaceFreedInBytes;

  public LocalStoreMonitorMetrics(String prefix, MetricsRegistry registry) {
    super(prefix, registry);
    diskSpaceFreedInBytes = newCounter("diskSpaceFreedInBytes");
    noOfDeletedTaskPartitionStores = newCounter("noOfDeletedTaskPartitionStores");
  }
}
