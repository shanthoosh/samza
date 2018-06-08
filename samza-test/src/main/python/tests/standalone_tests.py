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

import zopkio.constants as constants
from zopkio.deployer import Deployer, Process
from zopkio.remote_host_helper import better_exec_command, DeploymentError, get_sftp_client, get_ssh_client, open_remote_file, log_output, exec_with_env

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

    get_job_model(1)

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
    logger.info('Retrieved job model.')
    logger.info(job_model)
    return job_model

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

def execute_command(command):
    RECV_BLOCK_SIZE = 16
    HOST_NAME = 'localhost'
    with get_ssh_client(HOST_NAME, username=runtime.get_username(), password=runtime.get_password()) as ssh:
        chan = exec_with_env(ssh, command, msg="Failed to get PID", env={})
    output = chan.recv(RECV_BLOCK_SIZE)
    full_output = output
    while len(output) > 0:
        output = chan.recv(RECV_BLOCK_SIZE)
        full_output += output
    return full_output


def get_pid(process_name):
    pid_command = "ps aux | grep '{0}' | grep -v grep | tr -s ' ' | cut -d ' ' -f 2 | grep -Eo '[0-9]+'".format(process_name)
    non_failing_command = "{0}; if [ $? -le 1 ]; then true;  else false; fi;".format(pid_command)
    logger.info("Process id command: {0}.".format(pid_command))
    pids = []
    full_output = execute_command(non_failing_command)
    if len(full_output) > 0:
        pids = [int(pid_str) for pid_str in full_output.split('\n') if pid_str.isdigit()]

    return pids

def kill_process(pid):
    kill_command = "kill -9 {0}".format(pid)
    result = execute_command(kill_command)
    logger.info("Result of kill command: {0} is: {1}.".format(kill_command, result))

def pause_process(pid):
    pause_command = "kill -SIGSTOP {0}".format(pid)
    result = execute_command(pause_command)
    logger.info("Result of suspend command: {0} is: {1}.".format(pause_command, result))

def resume_process(pid):
    resume_command = "kill -CONT {0}".format(pid)
    result = execute_command(resume_command)
    logger.info("Result of suspend command: {0} is: {1}.".format(resume_command, result))

def _load_data():

    try:
       logger.info("load-data")
       deployer1 = util.get_deployer('standalone-processor-1')
       deployer2 = util.get_deployer('standalone-processor-2')
       deployer3 = util.get_deployer('standalone-processor-3')

       processor_1_ids = get_pid('standalone-processor-1')
       logger.info("Killing deployer-1 process: {0}.".format(processor_1_ids))
       for processor_1_id in processor_1_ids:
            kill_process(processor_1_id)
       processor_2_ids = get_pid('standalone-processor-2')
       logger.info("Killing deployer-2 process: {0}.".format(processor_2_ids))
       for processor_2_id in processor_2_ids:
            kill_process(processor_2_id)
       processor_3_ids = get_pid('standalone-processor-3')
       logger.info("Killing deployer-3 process: {0}.".format(processor_3_ids))
       for processor_3_id in processor_3_ids:
            kill_process(processor_3_id)

       logger.info("Starting processor 1.")
       for component in ['standalone-processor-1', 'standalone-processor-2', 'standalone-processor-3']:
            deployer = util.get_deployer(component)
            for instance, host in runtime.get_active_config(component + '_hosts').iteritems():
                logger.info('Deploying {0} on host: {1}'.format(instance, host))
                deployer.deploy(instance, {
                    'hostname': host
                })

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
