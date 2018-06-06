# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import util
import logging
import zopkio.runtime as runtime
from kafka import SimpleProducer, SimpleConsumer

logger = logging.getLogger(__name__)

JOB_ID = 'test-app-id'
PACKAGE_ID = 'tests'
TEST_INPUT_TOPIC = 'standaloneIntegrationTestKafkaInputTopic'
TEST_OUTPUT_TOPIC = 'standaloneIntegrationTestKafkaOutputTopic'
NUM_MESSAGES = 50

def test_samza_job():
    """
    Runs a job that reads converts input strings to integers, negates the
    integer, and outputs to a Kafka topic.
    """
    _load_data()

    deployer_name_to_config = {
        'standalone-processor-1' : 'config/standalone.failure.test-processor-1.properties',
        'standalone-processor-2' : 'config/standalone.failure.test-processor-2.properties',
        'standalone-processor-3' : 'config/standalone.failure.test-processor-3.properties'
    }

    for deployer in ['standalone-processor-1', 'standalone-processor-2', 'standalone-processor-3']:
        config_file = deployer_name_to_config[deployer]
        util.start_job(PACKAGE_ID, JOB_ID, config_file, deployer)
        util.await_job(PACKAGE_ID, JOB_ID, deployer)

def validate_samza_job():
    """
    Validates that negate-number negated all messages, and sent the output to
    samza-test-topic-output.
    """
    logger.info('Running validate_samza_job')
    # kafka = util.get_kafka_client()
    # consumer = SimpleConsumer(kafka, 'samza-test-group', TEST_OUTPUT_TOPIC)
    # messages = consumer.get_messages(count=NUM_MESSAGES, block=True, timeout=300)
    # message_count = len(messages)
    # kafka.close()

def _load_data():

    deployer1 = runtime.get_deployer('standalone-processor-1')
    deployer2 = runtime.get_deployer('standalone-processor-2')
    deployer3 = runtime.get_deployer('standalone-processor-3')

    for process in deployer1.get_processes:
        logger.info(process)

    logger.info("killing processor-1")
    deployer1.kill('standalone-processor-1')

    logger.info(deployer2.get_processes)
    logger.info(deployer3.get_processes)

    """
    Sends 50 messages (1 .. 50) to samza-test-topic.
    """
    logger.info('Running test_samza_job')
    kafka = util.get_kafka_client()
    kafka.ensure_topic_exists(TEST_INPUT_TOPIC)
    producer = SimpleProducer(
        kafka,
        async=False,
        req_acks=SimpleProducer.ACK_AFTER_CLUSTER_COMMIT,
        ack_timeout=30000)
    for i in range(1, NUM_MESSAGES + 1):
        logger.info('Publishing message to topic: {0}'.format(TEST_INPUT_TOPIC))
        producer.send_messages(TEST_INPUT_TOPIC, str(i))
    kafka.close()
