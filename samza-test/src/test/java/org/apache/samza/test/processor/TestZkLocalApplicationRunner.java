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

package org.apache.samza.test.processor;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.function.BiFunction;
import kafka.admin.AdminUtils;
import kafka.utils.TestUtils;
import org.I0Itec.zkclient.ZkClient;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.samza.SamzaException;
import org.apache.samza.application.StreamApplication;
import org.apache.samza.config.ApplicationConfig;
import org.apache.samza.config.Config;
import org.apache.samza.config.JobConfig;
import org.apache.samza.config.JobCoordinatorConfig;
import org.apache.samza.config.MapConfig;
import org.apache.samza.config.TaskConfig;
import org.apache.samza.config.ZkConfig;
import org.apache.samza.job.ApplicationStatus;
import org.apache.samza.job.model.JobModel;
import org.apache.samza.operators.MessageStream;
import org.apache.samza.operators.OutputStream;
import org.apache.samza.operators.StreamGraph;
import org.apache.samza.runtime.LocalApplicationRunner;
import org.apache.samza.test.StandaloneIntegrationTestHarness;
import org.apache.samza.test.StandaloneTestUtils;
import org.apache.samza.zk.ZkKeyBuilder;
import org.apache.samza.zk.ZkUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.*;

/**
 * Contains integration tests for {@link LocalApplicationRunner}.
 *
 * Brings up embedded ZooKeeper, Kafka broker and launches multiple {@link StreamApplication} through
 * {@link LocalApplicationRunner} to verify the guarantees made in stand alone execution environment.
 */
public class TestZkLocalApplicationRunner extends StandaloneIntegrationTestHarness {

  private static final int NUM_KAFKA_EVENTS = 100;
  private static final int ZK_CONNECTION_TIMEOUT_IN_MS = 10000;
  private static final Logger LOGGER = LoggerFactory.getLogger(TestZkLocalApplicationRunner.class);
  private static final String SYSTEM = "TestSystemName";
  private static final String TEST_SSP_GROUPER_FACTORY = "org.apache.samza.container.grouper.stream.GroupByPartitionFactory";
  private static final String TEST_TASK_GROUPER_FACTORY = "org.apache.samza.container.grouper.task.GroupByContainerIdsFactory";
  private static final String TEST_JOB_COORDINATOR_FACTORY = "org.apache.samza.zk.ZkJobCoordinatorFactory";
  private static final String TEST_SYSTEM_FACTORY = "org.apache.samza.system.kafka.KafkaSystemFactory";
  private static final String TEST_JOB_NAME = "test-job";
  private static final String[] PROCESSOR_IDS = new String[] {"0000000000", "0000000001", "0000000002"};

  private String testStreamAppName;
  private String testStreamAppId;
  private String inputKafkaTopic;
  private String outputKafkaTopic;
  private ZkClient zkClient;
  private ZkKeyBuilder zkKeyBuilder;
  private ZkUtils zkUtils;

  // Set 90 seconds as max execution time for each test.
  @Rule
  public Timeout testTimeOutInMillis = new Timeout(90000);

  public interface EventListener {
    void onMessageReceived(TestKafkaEvent message);
    void onExceptionOccurred(Throwable t);
  }

  private static class TestKafkaEvent implements Serializable {

    // Actual content of the event.
    private String eventData;

    // Contains Integer value, which is greater than previous message id.
    private String eventId;

    TestKafkaEvent(String eventId, String eventData) {
      this.eventData = eventData;
      this.eventId = eventId;
    }

    String getEventId() {
      return eventId;
    }

    String getEventData() {
      return eventData;
    }

    @Override
    public String toString() {
      return eventId + "|" + eventData ;
    }

    static TestKafkaEvent fromString(String message) {
      String[] messageComponents = message.split("|");
      return new TestKafkaEvent(messageComponents[0], messageComponents[1]);
    }
  }

  /**
   * Identity based stream application which emits event to EventListener when
   * processing messages and while handling exceptions.
   */
  private static class TestStreamApplication implements StreamApplication {

    private static final Logger LOGGER = LoggerFactory.getLogger(TestStreamApplication.class);

    private final String inputTopic;
    private final String outputTopic;
    private final CountDownLatch processedMessagesLatch;
    private final EventListener eventListener;

    TestStreamApplication(String inputTopic, String outputTopic,
        CountDownLatch processedMessagesLatch, EventListener eventListener) {
      this.inputTopic = inputTopic;
      this.outputTopic = outputTopic;
      this.processedMessagesLatch = processedMessagesLatch;
      this.eventListener = eventListener;
    }

    @Override
    public void init(StreamGraph graph, Config config) {
      try {
        BiFunction<String, String, String> messageBuilder = (key, msg) -> {
          String result = "";
          try {
            TestKafkaEvent incomingMessage = TestKafkaEvent.fromString(msg);
            if (eventListener != null) {
              eventListener.onMessageReceived(incomingMessage);
            }
            result = incomingMessage.toString();
          } catch (Exception e) {
            LOGGER.error("Exception when processing message in TestStreamApplication: {}", e);
            if (eventListener != null) {
            eventListener.onExceptionOccurred(e);
            }
          }
          if (processedMessagesLatch != null) {
            processedMessagesLatch.countDown();
          }
          return result;
        };
        MessageStream<String> inputStream = graph.getInputStream(inputTopic, messageBuilder);
        OutputStream<String, String, String> outputStream = graph.getOutputStream(outputTopic, event -> null, event -> event);
        inputStream.sendTo(outputStream);
      } catch (Exception e) {
        LOGGER.error("Exception when processing message in TestStreamApplication: {}", e);
        if (eventListener != null) {
          eventListener.onExceptionOccurred(e);
        }
      }
    }
  }

  List<String> readProcessorIdsFromZK(List<String> processorIds) {
    return zkUtils.getActiveProcessorsIDs(processorIds);
  }

  String readJobModelVersionFromZK() {
    return zkUtils.getJobModelVersion();
  }

  JobModel readJobModelFromZK() {
    return zkUtils.getJobModel(zkUtils.getJobModelVersion());
  }

  @Override
  public void setUp() {
    super.setUp();
    String randomString = UUID.randomUUID().toString();
    testStreamAppName = String.format("test-app-name-%s", randomString);
    testStreamAppId = String.format("test-app-id-%s", randomString);
    inputKafkaTopic = String.format("test-input-topic-%s", randomString);
    outputKafkaTopic = String.format("test-output-topic-%s", randomString);
    zkClient = new ZkClient(zkConnect());
    zkKeyBuilder = new ZkKeyBuilder(String.format("app-%s-%s", testStreamAppName, testStreamAppId));
    zkUtils = new ZkUtils(zkKeyBuilder, zkClient, ZK_CONNECTION_TIMEOUT_IN_MS);
    zkUtils.connect();

    for (String kafkaTopic : ImmutableList.of(inputKafkaTopic, outputKafkaTopic)) {
      LOGGER.info("Creating kafka topic: {}.", kafkaTopic);
      TestUtils.createTopic(zkUtils(), kafkaTopic, 5, 1, servers(), new Properties());
    }
  }

  @Override
  public void tearDown() {
    for (String kafkaTopic : ImmutableList.of(inputKafkaTopic, outputKafkaTopic)) {
      LOGGER.info("Deleting kafka topic: {}.", kafkaTopic);
      AdminUtils.deleteTopic(zkUtils(), kafkaTopic);
    }
    zkUtils.close();
    super.tearDown();
  }

  private void publishKafkaEvents(String topic, int numEvents, String streamProcessorId) {
    KafkaProducer producer = getKafkaProducer();
    for (int eventIndex = 0; eventIndex < numEvents; eventIndex++) {
      try {
        LOGGER.debug("Publish kafka event with index : {} for stream processor: {}.", eventIndex, streamProcessorId);
        producer.send(new ProducerRecord(topic, new TestKafkaEvent(streamProcessorId, String.valueOf(eventIndex)).toString().getBytes()));
      } catch (Exception  e) {
        LOGGER.error("Publishing to kafka topic: {} resulted in exception: {}.", new Object[]{topic, e});
        throw new SamzaException(e);
      }
    }
  }

  private ApplicationConfig buildStreamApplicationConfig(String systemName, String inputTopic,
                                                         String processorId, String appName, String appId) {
    Map<String, String> samzaContainerConfig = ImmutableMap.<String, String>builder()
        .put(TaskConfig.INPUT_STREAMS(), inputTopic)
        .put(JobConfig.JOB_DEFAULT_SYSTEM(), systemName)
        .put(TaskConfig.IGNORED_EXCEPTIONS(), "*")
        .put(ZkConfig.ZK_CONNECT, zkConnect())
        .put(JobConfig.SSP_GROUPER_FACTORY(), TEST_SSP_GROUPER_FACTORY)
        .put(TaskConfig.GROUPER_FACTORY(), TEST_TASK_GROUPER_FACTORY)
        .put(JobCoordinatorConfig.JOB_COORDINATOR_FACTORY, TEST_JOB_COORDINATOR_FACTORY)
        .put(JobConfig.PROCESSOR_ID(), processorId)
        .put(ApplicationConfig.APP_NAME, appName)
        .put(ApplicationConfig.APP_ID, appId)
        .put(String.format("systems.%s.samza.factory", systemName), TEST_SYSTEM_FACTORY)
        .put(JobConfig.JOB_NAME(), TEST_JOB_NAME)
        .build();
    Map<String, String> applicationConfig = Maps.newHashMap(samzaContainerConfig);
    applicationConfig.putAll(StandaloneTestUtils.getKafkaSystemConfigs(systemName, bootstrapServers(), zkConnect(), null, StandaloneTestUtils.SerdeAlias.STRING, true));
    return new ApplicationConfig(new MapConfig(applicationConfig));
  }

  @Test
  public void shouldReElectLeaderWhenLeaderDies() throws InterruptedException {
    // Set up kafka topics.
    publishKafkaEvents(inputKafkaTopic, NUM_KAFKA_EVENTS, PROCESSOR_IDS[0]);

    // Set up stream application configs with different processorIds and same testStreamAppName, testStreamAppId.
    ApplicationConfig applicationConfig1 = buildStreamApplicationConfig(SYSTEM, inputKafkaTopic, PROCESSOR_IDS[0], testStreamAppName, testStreamAppId);
    ApplicationConfig applicationConfig2 = buildStreamApplicationConfig(SYSTEM, inputKafkaTopic, PROCESSOR_IDS[1], testStreamAppName, testStreamAppId);
    ApplicationConfig applicationConfig3 = buildStreamApplicationConfig(SYSTEM, inputKafkaTopic, PROCESSOR_IDS[2], testStreamAppName, testStreamAppId);

    // Create stream applications.
    CountDownLatch processedMessagesLatch1 = new CountDownLatch(1);
    CountDownLatch processedMessagesLatch2 = new CountDownLatch(1);
    CountDownLatch processedMessagesLatch3 = new CountDownLatch(1);

    StreamApplication streamApp1 = new TestStreamApplication(inputKafkaTopic, outputKafkaTopic, processedMessagesLatch1, null);
    StreamApplication streamApp2 = new TestStreamApplication(inputKafkaTopic, outputKafkaTopic, processedMessagesLatch2, null);
    StreamApplication streamApp3 = new TestStreamApplication(inputKafkaTopic, outputKafkaTopic, processedMessagesLatch3, null);

    // Create local application runners.
    LocalApplicationRunner applicationRunner1 = new LocalApplicationRunner(applicationConfig1);
    LocalApplicationRunner applicationRunner2 = new LocalApplicationRunner(applicationConfig2);
    LocalApplicationRunner applicationRunner3 = new LocalApplicationRunner(applicationConfig3);

    // Run stream applications.
    applicationRunner1.run(streamApp1);
    applicationRunner2.run(streamApp2);
    applicationRunner3.run(streamApp3);

    // Wait until all processors have processed a message.
    processedMessagesLatch1.await();
    processedMessagesLatch2.await();
    processedMessagesLatch3.await();

    // Verifications before killing the leader.
    JobModel jobModel = readJobModelFromZK();
    String prevJobModelVersion = readJobModelVersionFromZK();
    assertEquals(3, jobModel.getContainers().size());
    assertEquals(Sets.newHashSet("0000000000", "0000000001", "0000000002"), jobModel.getContainers().keySet());
    assertEquals("1", prevJobModelVersion);

    List<String> processorIdsFromZK = readProcessorIdsFromZK(Arrays.asList(PROCESSOR_IDS));

    assertEquals(3, processorIdsFromZK.size());
    assertEquals(PROCESSOR_IDS[0], processorIdsFromZK.get(0));

    // Kill the leader. Since streamApp1 is the first to join the cluster, it's the leader.
    applicationRunner1.kill(streamApp1);
    applicationRunner1.waitForFinish();

    // Verifications after killing the leader.
    assertEquals(ApplicationStatus.SuccessfulFinish, applicationRunner1.status(streamApp1));
    processorIdsFromZK = readProcessorIdsFromZK(ImmutableList.of(PROCESSOR_IDS[1], PROCESSOR_IDS[2]));
    assertEquals(2, processorIdsFromZK.size());
    assertEquals(PROCESSOR_IDS[1], processorIdsFromZK.get(0));
    jobModel = readJobModelFromZK();
    assertEquals(Sets.newHashSet( "0000000001", "0000000002"), jobModel.getContainers().keySet());
    String currentJobModelVersion = readJobModelVersionFromZK();
    assertEquals(2, jobModel.getContainers().size());
    assertEquals("2", currentJobModelVersion);
  }

  // Checks enforcing property that all processors should have unique Id. Commented out since exceptions are not propagated up.
  // @Test(expected = Exception.class)
  public void shouldFailWhenNewProcessorJoinsWithSameIdAsExistingProcessor() throws InterruptedException {
    // Set up kafka topics.
    publishKafkaEvents(inputKafkaTopic, NUM_KAFKA_EVENTS, PROCESSOR_IDS[0]);

    // Create configurations.
    ApplicationConfig applicationConfig1 = buildStreamApplicationConfig(SYSTEM, inputKafkaTopic, PROCESSOR_IDS[0], testStreamAppName, testStreamAppId);
    ApplicationConfig applicationConfig2 = buildStreamApplicationConfig(SYSTEM, inputKafkaTopic, PROCESSOR_IDS[1], testStreamAppName, testStreamAppId);

    // Create StreamApplications.
    CountDownLatch processedMessagesLatch1 = new CountDownLatch(1);
    CountDownLatch processedMessagesLatch2 = new CountDownLatch(1);

    StreamApplication streamApp1 = new TestStreamApplication(inputKafkaTopic, outputKafkaTopic, processedMessagesLatch1, null);
    StreamApplication streamApp2 = new TestStreamApplication(inputKafkaTopic, outputKafkaTopic, processedMessagesLatch2, null);

    // Create LocalApplicationRunners.
    LocalApplicationRunner applicationRunner1 = new LocalApplicationRunner(applicationConfig1);
    LocalApplicationRunner applicationRunner2 = new LocalApplicationRunner(applicationConfig2);

    // Run stream applications.
    applicationRunner1.run(streamApp1);
    applicationRunner2.run(streamApp2);

    // Wait for message processing to start in both the processors.
    processedMessagesLatch1.await();
    processedMessagesLatch2.await();

    LocalApplicationRunner applicationRunner3 = new LocalApplicationRunner(new MapConfig(applicationConfig2));

    // Create a stream app with same processor id as SP2 and run it. It should fail.
    StreamApplication streamApp3 = new TestStreamApplication(inputKafkaTopic, outputKafkaTopic, null, null);
    applicationRunner3.run(streamApp3);

    // The following line should throw up by handling duplicate processorId registration.
    applicationRunner3.waitForFinish();
  }


  // This is commented out because this exception is not handled currently.
//  @Test(expected = Exception.class)
  public void shouldKillStreamAppWhenZooKeeperDiesBeforeLeaderReElection() throws InterruptedException {
    // Create configurations.
    ApplicationConfig applicationConfig1 = buildStreamApplicationConfig(SYSTEM, inputKafkaTopic, PROCESSOR_IDS[0], testStreamAppName, testStreamAppId);
    ApplicationConfig applicationConfig2 = buildStreamApplicationConfig(SYSTEM, inputKafkaTopic, PROCESSOR_IDS[1], testStreamAppName, testStreamAppId);

    // Create StreamApplications.
    StreamApplication streamApp1 = new TestStreamApplication(inputKafkaTopic, outputKafkaTopic, null, null);
    StreamApplication streamApp2 = new TestStreamApplication(inputKafkaTopic, outputKafkaTopic, null, null);

    // Create LocalApplicationRunners.
    LocalApplicationRunner applicationRunner1 = new LocalApplicationRunner(applicationConfig1);
    LocalApplicationRunner applicationRunner2 = new LocalApplicationRunner(applicationConfig2);

    // Run stream applications.
    applicationRunner1.run(streamApp1);
    applicationRunner2.run(streamApp2);

    applicationRunner1.kill(streamApp1);
    applicationRunner1.waitForFinish();
    assertEquals(ApplicationStatus.SuccessfulFinish, applicationRunner1.status(streamApp2));

    // Kill zookeeper server before debounce time. JobModel regeneration and ZK fencing will fail with exception.
    zookeeper().shutdown();

    applicationRunner1 = new LocalApplicationRunner(applicationConfig1);
    applicationRunner1.run(streamApp1);
    // This line should throw exception since Zookeeper is unavailable.
    applicationRunner1.waitForFinish();
  }

  /**
   * Commented out since killing and starting stream app causes failure at ZkBarrierVersion upgrade.
   * Trying to reconnect using closed ZkClient.
   .*/
//   @Test
  public void testRollingUpgrade() throws Exception {
    publishKafkaEvents(inputKafkaTopic, NUM_KAFKA_EVENTS, PROCESSOR_IDS[0]);

    // Create stream app configuration.
    ApplicationConfig applicationConfig1 = buildStreamApplicationConfig(SYSTEM, inputKafkaTopic, PROCESSOR_IDS[0], testStreamAppName, testStreamAppId);
    ApplicationConfig applicationConfig2 = buildStreamApplicationConfig(SYSTEM, inputKafkaTopic, PROCESSOR_IDS[1], testStreamAppName, testStreamAppId);

    final List<TestKafkaEvent> messagesProcessed = new ArrayList<>();

    EventListener eventListener = new EventListener() {
      @Override
      public void onMessageReceived(TestKafkaEvent message) {
        messagesProcessed.add(message);
      }
      @Override
      public void onExceptionOccurred(Throwable t) {}
    };

    // Create StreamApplication from configuration.
    CountDownLatch processedMessagesLatch1 = new CountDownLatch(1);
    CountDownLatch processedMessagesLatch2 = new CountDownLatch(1);

    StreamApplication streamApp1 = new TestStreamApplication(inputKafkaTopic, outputKafkaTopic, processedMessagesLatch1, eventListener);
    StreamApplication streamApp2 = new TestStreamApplication(inputKafkaTopic, outputKafkaTopic, processedMessagesLatch2,null);

    // Create LocalApplicationRunner's
    LocalApplicationRunner applicationRunner1 = new LocalApplicationRunner(applicationConfig1);
    LocalApplicationRunner applicationRunner2 = new LocalApplicationRunner(applicationConfig2);

    // Run stream application.
    applicationRunner1.run(streamApp1);
    applicationRunner2.run(streamApp2);

    processedMessagesLatch1.await();
    processedMessagesLatch2.await();

    applicationRunner1.kill(streamApp1);

    int lastProcessedMessageId = -1;
    for (TestKafkaEvent message : messagesProcessed) {
      lastProcessedMessageId = Math.max(lastProcessedMessageId, Integer.parseInt(message.getEventId()));
    }

    applicationRunner1 = new LocalApplicationRunner(applicationConfig1);
    applicationRunner1.run(streamApp1);

    applicationRunner1.waitForFinish();

    // Kill both the stream applications.
    applicationRunner1.kill(streamApp1);
    applicationRunner2.kill(streamApp2);

    // This should be continuation of last processed message.
    int nextSeenMessageId = Integer.parseInt(messagesProcessed.get(0).getEventId());
    assertTrue(lastProcessedMessageId < nextSeenMessageId);
  }
}
