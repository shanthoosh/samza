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
import sys
import logging
import zopkio.runtime as runtime
from kafka import SimpleProducer, SimpleConsumer
import struct
import os
import time
import zipfile
import urllib
import traceback
from subprocess import call
from kazoo.client import KazooClient
import json
import zopkio.constants as constants
from zopkio.deployer import Deployer, Process
from zopkio.remote_host_helper import better_exec_command, DeploymentError, get_sftp_client, get_ssh_client, open_remote_file, log_output, exec_with_env
from standalone_processor import StandaloneProcessor

logger = logging.getLogger(__name__)

APP_NAME = 'test-app-name'
APP_ID = 'test-app-id'
ZK_BASE_DIR='app-{0}-{1}/{2}-{3}-coordinationData'.format(APP_NAME, APP_ID, APP_NAME, APP_ID)

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

    job_model_dict = get_job_model(1)
    logger.info("Job model dict: {0}".format(job_model_dict))

    deployer_name_to_config = {
        'standalone-processor-1' : 'config/standalone.failure.test-processor-1.properties',
        'standalone-processor-2' : 'config/standalone.failure.test-processor-2.properties',
        'standalone-processor-3' : 'config/standalone.failure.test-processor-3.properties'
    }

    for deployer in ['standalone-processor-1', 'standalone-processor-2', 'standalone-processor-3']:
        config_file = deployer_name_to_config[deployer]
        logger.info(deployer)
        logger.info(config_file)

def get_job_model(jm_version):
    zk_client = KazooClient(hosts='127.0.0.1:2181')
    zk_client.start()
    logger.info("Fetching the JobModel with version: {0}.".format(jm_version))
    logger.info("Invoking getChildren on path: {0}".format(ZK_BASE_DIR))
    job_model_generation_path = '{0}/JobModelGeneration/jobModels/{1}'.format(ZK_BASE_DIR, jm_version)
    job_model, _ = zk_client.get(job_model_generation_path)

    """ 
    Dirty hack: Inbuilt data serializers in ZkClient java library persist data in the following format in zookeeper data nodes:
    
            class_name, data_length, actual_data
    
    JobModel json manipulation: Delete all the characters before first occurrence of '{' in jobModel json string.
    
    Primitive json deserialization without the above custom string massaging fails. This will be removed after SAMZA-1876.
    
    """

    first_curly_index = job_model.find('{')
    job_model = job_model[first_curly_index: ]

    logger.info('Retrieved job model.')
    logger.info(job_model)
    job_model_dict = json.loads(job_model)
    return job_model_dict

def validate_standalone_job():
    """
    Validates that negate-number negated all messages, and sent the output to
    samza-test-topic-output.
    """
    logger.info('Running validate_samza_job')
    kafka = util.get_kafka_client()
    consumer = SimpleConsumer(kafka, 'samza-test-group', TEST_OUTPUT_TOPIC)
    logger.info("Reading messages from topic: {0}.".format(TEST_OUTPUT_TOPIC))
    messages = consumer.get_messages(count=NUM_MESSAGES, block=True, timeout=300)
    message_count = len(messages)
    logger.info("Messages read count: {0}.".format(message_count))
    assert NUM_MESSAGES == message_count, 'Expected {0} lines, but found {1}'.format(NUM_MESSAGES, message_count)
    kafka.close()

def _load_data():

    try:
       logger.info("load-data")

       processors = []
       for processor_id in ['standalone-processor-1', 'standalone-processor-2', 'standalone-processor-3']:
           processors.append(StandaloneProcessor(processor_id=processor_id, package_id=PACKAGE_ID, configs={}))

       for processor in processors:
           processor.deploy()
           processor_id = processor.get_processor_id()
           logger.info("Killing processor with id: {0}.".format(processor_id))
           processor.kill()

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
    except:
       logger.error(traceback.format_exc(sys.exc_info()))
