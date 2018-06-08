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
import zk_util

logger = logging.getLogger(__name__)

APP_NAME = 'test-app-name'
APP_ID = 'test-app-id'
ZK_BASE_DIR='app-{0}-{1}/{2}-{3}-coordinationData'.format(APP_NAME, APP_ID, APP_NAME, APP_ID)

PACKAGE_ID = 'tests'
TEST_INPUT_TOPIC = 'standaloneIntegrationTestKafkaInputTopic'
TEST_OUTPUT_TOPIC = 'standaloneIntegrationTestKafkaOutputTopic'
NUM_MESSAGES = 50
JOB_MODEL_TIMEOUT = 20

def test_samza_job():
    """
    Runs a job that reads converts input strings to integers, negates the
    integer, and outputs to a Kafka topic.
    """
    _load_data()

    job_model_dict = zk_util.get_latest_job_model(ZK_BASE_DIR)

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

def test_kill_current_master():
    try:
        logger.info("Executing kill current master test!")
        processors = {}
        for processor_id in ['standalone-processor-1', 'standalone-processor-2', 'standalone-processor-3']:
            processors[processor_id] = StandaloneProcessor(processor_id=processor_id, package_id=PACKAGE_ID, configs={})
            processors[processor_id].deploy()

        leader_processor_id = zk_util.get_leader_processor_id(zk_base_dir=ZK_BASE_DIR)
        leader_processor = processors[leader_processor_id]

        leader_processor.kill()

        ## Wait for new JobModel to be published.
        time.sleep(JOB_MODEL_TIMEOUT)

        job_model = zk_util.get_latest_job_model(zk_base_dir=ZK_BASE_DIR)

        assert 2 == len(job_model['containers']), 'Expected processor count: {0}, actual processor count: {1}.'.format(2, len(job_model['containers']))

        for processor_id, processor in processors:
            logger.info("Killing processor: {0}.".format(processor_id))
            processor.kill()
    finally:
        logger.error(traceback.format_exc(sys.exc_info()))

def _load_data():
    kafka = None
    try:
       logger.info("load-data")
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
    except:
       logger.error(traceback.format_exc(sys.exc_info()))
    finally:
        if kafka is not None:
            kafka.close()
