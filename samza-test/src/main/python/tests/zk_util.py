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
import util
import logging
import socket
import time
import errno
import os
import zopkio.runtime as runtime
from kafka import KafkaClient, SimpleProducer, SimpleConsumer
import zopkio.adhoc_deployer as adhoc_deployer
from kazoo.client import KazooClient
import json

logger = logging.getLogger(__name__)

##
## TODO: Add docs.
##
def get_job_model_version(zk_base_dir):
    zk_client = None
    try:
        zk_client = KazooClient(hosts='127.0.0.1:2181')
        zk_client.start()
        job_model_version_path = '{0}/JobModelGeneration/jobModelVersion'.format(zk_base_dir)
        job_model_version, stat = zk_client.get(job_model_version_path)
        logger.info('Retrieved job model version: {0}, stat.version: {1}.'.format(job_model_version, stat.version))
        return stat.version
    finally:
        if zk_client is not None:
            zk_client.stop()

##
## TODO: Add docs.
##
def get_job_model(zk_base_dir, jm_version):
    zk_client = None
    try:
        zk_client = KazooClient(hosts='127.0.0.1:2181')
        zk_client.start()

        logger.info("Fetching the JobModel with version: {0}.".format(jm_version))
        job_model_generation_path = '{0}/JobModelGeneration/jobModels/{1}'.format(zk_base_dir, jm_version)
        job_model, _ = zk_client.get(job_model_generation_path)

        """ 
        Inbuilt data serializers in ZkClient java library persist data in the following format in zookeeper data nodes:
                    class_name, data_length, actual_data
    
        JobModel json manipulation: Delete all the characters before first occurrence of '{' in jobModel json string.
        Primitive json deserialization fails without the above custom string massaging. This will be removed after SAMZA-1876.
        """

        first_curly_index = job_model.find('{')
        job_model = job_model[first_curly_index: ]

        logger.info('Retrieved job model.')
        logger.info(job_model)
        job_model_dict = json.loads(job_model)
        return job_model_dict
    finally:
        if zk_client is not None:
            zk_client.stop()

##
## TODO: Add docs.
##
def validate_output_stream(topic_name, expected_message_count):
    """
    Validates that presence of {expected_message_count} messages in topic_name.
    """
    kafka_client = None
    try:
        logger.info('Running validate_output_stream')
        kafka_client = util.get_kafka_client()
        consumer = SimpleConsumer(kafka_client, 'samza-test-group', topic_name)
        logger.info("Reading messages from topic: {0}.".format(topic_name))
        messages = consumer.get_messages(count=expected_message_count, block=True, timeout=300)
        actual_message_count = len(messages)
        logger.info("Messages read count: {0}.".format(actual_message_count))
        assert expected_message_count == actual_message_count, 'Expected messages: {0}, actual messages: {1}.'.format(expected_message_count, actual_message_count)
    finally:
        if kafka_client is not None:
            kafka_client.close()

##
## TODO: Add docs.
##
def get_leader_processor_id(zk_base_dir):
    zk_client = None
    try:
        logger.info("Executing get_leader_processor_id: {0}.".format(zk_base_dir))
        zk_client = KazooClient(hosts='127.0.0.1:2181')
        zk_client.start()
        processors_path =  '{0}/processors'.format(zk_base_dir)

        logger.info("Invoking getChildren on path: {0}".format(processors_path))

        childZkNodes = zk_client.get_children(processors_path)
        logger.info("ChildZNodes of parent path: {0} is {1}.".format(processors_path, childZkNodes))
        processor_ids = []
        for childZkNode in childZkNodes:
            child_processor_path = '{0}/{1}'.format(processors_path, childZkNode)
            logger.info("Invoking getData on path: {0}".format(child_processor_path))
            processor_data, _ = zk_client.get(child_processor_path)
            host, processor_id = processor_data.split(" ")
            processor_ids.append(processor_id)

        logger.info('Retrieved processors.')
        logger.info(processor_ids)
        if len(processor_ids) > 0:
            return processor_ids[0]
        else:
            return None
    finally:
        if zk_client is not None:
            zk_client.stop()
