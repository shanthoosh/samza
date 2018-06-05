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

package org.apache.samza.test.integration;

import joptsimple.OptionSet;
import org.apache.samza.application.StreamApplication;
import org.apache.samza.config.Config;
import org.apache.samza.runtime.ApplicationRunner;
import org.apache.samza.runtime.ApplicationRunnerMain;
import org.apache.samza.runtime.ApplicationRunnerOperation;
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

    ApplicationRunnerOperation op = cmdLine.getOperation(options);

    try {
      if (op.equals(ApplicationRunnerOperation.RUN)) {

        Runnable runnable = () -> {
          try {
            runner.run(app);
          } catch (Exception e) {
            LOGGER.error("Exception occurred: ", e);
          }
        };
        Thread thread = new Thread(runnable);
        thread.start();
      } else if (op.equals(ApplicationRunnerOperation.KILL)) {
        runner.kill(app);
        runner.waitForFinish();
      }
    } catch (Exception e) {
      LOGGER.error("Exception occurred when invoking: {} on application: {}.", op, app, e);
    }
  }
}
