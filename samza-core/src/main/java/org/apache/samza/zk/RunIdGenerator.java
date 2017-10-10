package org.apache.samza.zk;

import org.apache.samza.config.Config;


/**
 * TODO: Add class doc.
 */
public interface RunIdGenerator {

  /**
   * TODO: Add documentation.
   * @param config
   * @return
   */
  String generateRunId(Config config);
}
