package org.apache.samza.test.integration;

import joptsimple.OptionSet;
import org.apache.samza.application.StreamApplication;
import org.apache.samza.config.Config;
import org.apache.samza.runtime.ApplicationRunner;
import org.apache.samza.runtime.ApplicationRunnerMain;
import org.apache.samza.util.Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.samza.runtime.ApplicationRunnerMain.STREAM_APPLICATION_CLASS_CONFIG;

/**
 * ApplicationRunnerMain is built for yarn deployment and doesn't work for standalone.
 * Created for standalone failure tests.
 */
public class LocalApplicationRunnerMain {

  private static final Logger LOGGER = LoggerFactory.getLogger(LocalApplicationRunnerMain.class);

  public static void main(String[] args) throws Exception {
      ApplicationRunnerMain.ApplicationRunnerCommandLine cmdLine = new ApplicationRunnerMain.ApplicationRunnerCommandLine();
      OptionSet options = cmdLine.parser().parse(args);
      Config orgConfig = cmdLine.loadConfig(options);
      Config config = Util.rewriteConfig(orgConfig);

      ApplicationRunner runner = ApplicationRunner.fromConfig(config);
      StreamApplication app = (StreamApplication) Class.forName(config.get(STREAM_APPLICATION_CLASS_CONFIG)).newInstance();

      try {
      runner.run(app);
      runner.waitForFinish();
      } catch (Exception e) {
        LOGGER.error("Exception occurred when launching stream application: {}.", app, e);
      }
  }
}
