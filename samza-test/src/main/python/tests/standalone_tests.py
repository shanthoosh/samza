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
JOB_MODEL_TIMEOUT = 6

def test_kill_master():
    try:
        logger.info("Executing kill current master test!")
        _load_data()
        processors = {}
        for processor_id in ['standalone-processor-1', 'standalone-processor-2', 'standalone-processor-3']:
            processors[processor_id] = StandaloneProcessor(processor_id=processor_id, package_id=PACKAGE_ID, configs={})
            processors[processor_id].deploy()

        leader_processor_id = zk_util.get_leader_processor_id(zk_base_dir=ZK_BASE_DIR)
        leader_processor = processors.pop(leader_processor_id)
        leader_processor.kill()

        ## Wait for new JobModel to be published.
        time.sleep(JOB_MODEL_TIMEOUT)

        job_model = zk_util.get_latest_job_model(zk_base_dir=ZK_BASE_DIR)

        logger.info('JobModel dict received: {0}'.format(job_model))

        containers = job_model['containers']

        logger.info('Containers: {0}.'.format(containers))

        for processor_id, deployer in processors.iteritems():
            logger.info('Checking: {0} for processor_id: {1}.'.format(containers, processor_id))
            assert(processor_id in containers, 'Processor id: {0} doesnt exist in JobModel.'.format(processor_id))

        for processor_id, processor in processors.iteritems():
            logger.info("Killing processor: {0}.".format(processor_id))
            processor.kill()
    finally:
        logger.error(traceback.format_exc(sys.exc_info()))

def test_kill_single_worker():
    try:
        logger.info("Executing kill current master test!")
        _load_data()
        processors = {}
        for processor_id in ['standalone-processor-1', 'standalone-processor-2', 'standalone-processor-3']:
            processors[processor_id] = StandaloneProcessor(processor_id=processor_id, package_id=PACKAGE_ID, configs={})
            processors[processor_id].deploy()

        leader_processor_id = zk_util.get_leader_processor_id(zk_base_dir=ZK_BASE_DIR)
        for processor_id, deployer in processors.iteritems():
            if processor_id != leader_processor_id:
                logger.info('Killing processor_id: {0}.'.format(processor_id))
                follower = processors.pop(processor_id)
                follower.kill()
                break

        ## Wait for new JobModel to be published.
        time.sleep(JOB_MODEL_TIMEOUT)

        job_model = zk_util.get_latest_job_model(zk_base_dir=ZK_BASE_DIR)
        logger.info('JobModel dict received: {0}'.format(job_model))
        containers = job_model['containers']

        logger.info('Containers: {0}.'.format(containers))

        for processor_id, deployer in processors.iteritems():
            logger.info('Checking: {0} for processor_id: {1}.'.format(containers, processor_id))
            assert(processor_id in containers, 'Processor id: {0} doesnt exist in JobModel.'.format(processor_id))

        for processor_id, processor in processors.iteritems():
            logger.info("Killing processor: {0}.".format(processor_id))
            processor.kill()
    finally:
        logger.error(traceback.format_exc(sys.exc_info()))



def test_kill_multiple_workers():
    try:
        logger.info("Executing kill current master test!")
        _load_data()
        processors = {}
        for processor_id in ['standalone-processor-1', 'standalone-processor-2', 'standalone-processor-3']:
            processors[processor_id] = StandaloneProcessor(processor_id=processor_id, package_id=PACKAGE_ID, configs={})
            processors[processor_id].deploy()

        leader_processor_id = zk_util.get_leader_processor_id(zk_base_dir=ZK_BASE_DIR)
        for processor_id, deployer in processors.iteritems():
            if processor_id != leader_processor_id:
                logger.info('Killing processor_id: {0}.'.format(processor_id))
                follower = processors.pop(processor_id)
                follower.kill()

        ## Wait for new JobModel to be published.
        time.sleep(JOB_MODEL_TIMEOUT)

        job_model = zk_util.get_latest_job_model(zk_base_dir=ZK_BASE_DIR)
        logger.info('JobModel dict received: {0}'.format(job_model))
        containers = job_model['containers']

        logger.info('Containers: {0}.'.format(containers))

        for processor_id, deployer in processors.iteritems():
            logger.info('Checking: {0} for processor_id: {1}.'.format(containers, processor_id))
            if processor_id != leader_processor_id:
                assert(processor_id in containers, 'Processor id: {0} doesnt exist in JobModel.'.format(processor_id))

        for processor_id, processor in processors.iteritems():
            logger.info("Killing processor: {0}.".format(processor_id))
            processor.kill()
    finally:
        logger.error(traceback.format_exc(sys.exc_info()))

def test_kill_leader_and_follower():
    try:
        logger.info("Executing kill current master test!")
        _load_data()
        processors = {}
        for processor_id in ['standalone-processor-1', 'standalone-processor-2', 'standalone-processor-3']:
            processors[processor_id] = StandaloneProcessor(processor_id=processor_id, package_id=PACKAGE_ID, configs={})
            processors[processor_id].deploy()

        leader_processor_id = zk_util.get_leader_processor_id(zk_base_dir=ZK_BASE_DIR)
        for processor_id, deployer in processors.iteritems():
            if processor_id != leader_processor_id:
                logger.info('Killing processor_id: {0}.'.format(processor_id))
                follower = processors.pop(processor_id)
                follower.kill()
                break

        leader_processor = processors.pop(leader_processor_id)
        leader_processor.kill()

        ## Wait for new JobModel to be published.
        time.sleep(JOB_MODEL_TIMEOUT)

        job_model = zk_util.get_latest_job_model(zk_base_dir=ZK_BASE_DIR)
        logger.info('JobModel dict received: {0}'.format(job_model))
        containers = job_model['containers']

        logger.info('Containers: {0}.'.format(containers))

        assert(leader_processor_id in containers, 'Processor id: {0} doesnt exist in JobModel.'.format(leader_processor_id))

        for processor_id, processor in processors.iteritems():
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
