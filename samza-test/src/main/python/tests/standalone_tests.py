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
import os
import shutil
import unittest

from zopkio.deployer import Deployer, Process
from zopkio.remote_host_helper import ParamikoError, better_exec_command, get_ssh_client, copy_dir, get_sftp_client

class TestDeployer(unittest.TestCase):

    logger = logging.getLogger(__name__)

    APP_NAME = 'test-app-name'
    APP_ID = 'test-app-id'
    ZK_BASE_DIR='app-{0}-{1}/{2}-{3}-coordinationData'.format(APP_NAME, APP_ID, APP_NAME, APP_ID)

    PACKAGE_ID = 'tests'
    TEST_INPUT_TOPIC = 'standaloneIntegrationTestKafkaInputTopic'
    TEST_OUTPUT_TOPIC = 'standaloneIntegrationTestKafkaOutputTopic'
    NUM_MESSAGES = 50
    JOB_MODEL_TIMEOUT = 6
    processors = {}

    def _load_data(self):
        kafka = None
        try:
            logger.info("load-data")
            kafka = util.get_kafka_client()
            kafka.ensure_topic_exists(TEST_INPUT_TOPIC)
            producer = SimpleProducer(kafka, async=False, req_acks=SimpleProducer.ACK_AFTER_CLUSTER_COMMIT, ack_timeout=30000)
            for message_index in range(1, NUM_MESSAGES + 1):
                logger.info('Publishing message to topic: {0}'.format(TEST_INPUT_TOPIC))
                producer.send_messages(TEST_INPUT_TOPIC, str(message_index))
        except:
            logger.error(traceback.format_exc(sys.exc_info()))
        finally:
            if kafka is not None:
                kafka.close()

    def setup(self):
        global processors
        _load_data()
        processors = {}
        for processor_id in ['standalone-processor-1', 'standalone-processor-2', 'standalone-processor-3']:
            processors[processor_id] = StandaloneProcessor(processor_id=processor_id, package_id=PACKAGE_ID, configs={})
            processors[processor_id].deploy()

    def teardown(self):
        global processors

        for processor_id, processor in processors.iteritems():
            logger.info("Killing processor: {0}.".format(processor_id))
            processor.kill()

    def test_kill_master(self):
        try:
            leader_processor_id = zk_util.get_leader_processor_id(zk_base_dir=ZK_BASE_DIR)
            processors.pop(leader_processor_id).kill()

            time.sleep(JOB_MODEL_TIMEOUT)

            job_model = zk_util.get_latest_job_model(zk_base_dir=ZK_BASE_DIR)

            for processor_id, deployer in processors.iteritems():
                assert processor_id in job_model['containers'], 'Processor id: {0} does not exist in JobModel.'.format(processor_id)
        finally:
            logger.error(traceback.format_exc(sys.exc_info()))

    def test_kill_single_worker(self):
        try:
            leader_processor_id = zk_util.get_leader_processor_id(zk_base_dir=ZK_BASE_DIR)
            for processor_id, deployer in processors.iteritems():
                if processor_id != leader_processor_id:
                    processors.pop(processor_id).kill()
                    break

            time.sleep(JOB_MODEL_TIMEOUT)
            job_model = zk_util.get_latest_job_model(zk_base_dir=ZK_BASE_DIR)
            for processor_id, deployer in processors.iteritems():
                assert processor_id in job_model['containers'], 'Processor id: {0} does not exist in JobModel.'.format(processor_id)
        finally:
            logger.error(traceback.format_exc(sys.exc_info()))

    def test_kill_multiple_workers(self):
        try:
            leader_processor_id = zk_util.get_leader_processor_id(zk_base_dir=ZK_BASE_DIR)
            for processor_id in processors.keys():
                if processor_id != leader_processor_id:
                    follower = processors.pop(processor_id)
                    follower.kill()

            time.sleep(JOB_MODEL_TIMEOUT)
            job_model = zk_util.get_latest_job_model(zk_base_dir=ZK_BASE_DIR)
            assert leader_processor_id in job_model['containers'], 'Processor id: {0} does not exist in JobModel.'.format(leader_processor_id)
        finally:
            logger.error(traceback.format_exc(sys.exc_info()))

    def test_kill_leader_and_follower(self):
        try:
            leader_processor_id = zk_util.get_leader_processor_id(zk_base_dir=ZK_BASE_DIR)
            processors.pop(leader_processor_id).kill()

            for processor_id in processors.keys():
                processors.pop(processor_id).kill()
                break

            time.sleep(JOB_MODEL_TIMEOUT)
            job_model = zk_util.get_latest_job_model(zk_base_dir=ZK_BASE_DIR)
            assert leader_processor_id in job_model['containers'], 'Processor id: {0} does not exist in JobModel.'.format(leader_processor_id)
        finally:
            logger.error(traceback.format_exc(sys.exc_info()))

if __name__ == '__main__':
  unittest.main()
