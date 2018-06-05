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

import org.apache.samza.application.StreamApplication;
import org.apache.samza.config.Config;
import org.apache.samza.operators.StreamGraph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PassThroughFilter implements StreamApplication {

  private static final Logger LOGGER = LoggerFactory.getLogger(PassThroughFilter.class);

  @Override
  public void init(StreamGraph graph, Config config) {
    String inputStream = config.get("input.stream.name");
    String outputStreamName = "testKafkaOutputTopic";
    LOGGER.info("Publishing message to: {}.", outputStreamName);
    graph.getInputStream(inputStream).sendTo(graph.getOutputStream(outputStreamName));
  }
}
