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

import os
import logging
import urllib
import time
import zopkio.runtime as runtime
import zopkio.adhoc_deployer as adhoc_deployer
from zopkio.runtime import get_active_config as c
from subprocess import PIPE, Popen
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

TEST_INPUT_TOPIC = 'standaloneIntegrationTestKafkaInputTopic'
TEST_OUTPUT_TOPIC = 'standaloneIntegrationTestKafkaOutputTopic'

logger = logging.getLogger(__name__)
deployers = {}

### Download the components if unavailable in deployment directory using url defined in config.
def _download_components(components):
    for component in components:
        url_key = 'url_{0}'.format(component)
        url = c(url_key)
        filename = os.path.basename(url)
        if os.path.exists(filename):
            logger.debug('Using cached file: {0}.'.format(filename))
        else:
            logger.info('Downloading {0} from {1}.'.format(component, url))
            urllib.urlretrieve(url, filename)

### Install and start all the components(from param) through binaries in deployment directory.
def _deploy_components(components):
    global deployers

    for component in components:
        deployer = adhoc_deployer.SSHDeployer(component, {
            'install_path': os.path.join(c('remote_install_path'), c(component + '_install_path')),
            'executable': c(component + '_executable'),
            'post_install_cmds': c(component + '_post_install_cmds', []),
            'start_command': c(component + '_start_cmd'),
            'stop_command': c(component + '_stop_cmd'),
            'extract': True,
            'sync': True,
        })
        deployers[component] = deployer
        runtime.set_deployer(component, deployer)
        for instance, host in c(component + '_hosts').iteritems():
            logger.info('Deploying {0} on host: {1}'.format(instance, host))
            deployer.deploy(instance, {
                'hostname': host
            })

            time.sleep(10)

## zookeeper_servers: Comma seperated list of zookeeper servers of the form host:port.
## topic_name: kafka topic to be created.
## partition_count: Number of partitions of the kafka topic.
## replication_factor: Replication factor of the kafka topic.
def _create_kafka_topic(zookeeper_servers, topic_name, partition_count, replication_factor):

    ### Using command line utility to create kafka topic since kafka python API doesn't support configuring partitionCount during topic creation.
    base_dir = os.getcwd()
    logger.info('Current working directory: {0}'.format(base_dir))

    create_topic_command = 'sh {0}/deploy/kafka/kafka_2.10-0.10.1.1/bin/kafka-topics.sh --create --zookeeper {1} --replication-factor {2} --partitions {3} --topic {4}'.format(base_dir, zookeeper_servers, replication_factor, partition_count, topic_name)
    logger.info("Creating topic: {0}.".format(topic_name))
    p = Popen(create_topic_command.split(' '), stdin=PIPE, stdout=PIPE, stderr=PIPE)
    output, err = p.communicate()
    logger.info("Output from create kafka topic: {0}\nstdout: {1}\nstderr: {2}".format(topic_name, output, err))

## Delete kafka topic
def _delete_kafka_topic(zookeeper_servers, topic_name):
    base_dir = os.getcwd()
    delete_topic_command = 'sh {0}/deploy/kafka/kafka_2.10-0.10.1.1/bin/kafka-topics.sh --delete --zookeeper {1} --topic {2}'.format(base_dir, zookeeper_servers, topic_name)
    logger.info("Deleting topic: {0}.".format(topic_name))
    p = Popen(delete_topic_command.split(' '), stdin=PIPE, stdout=PIPE, stderr=PIPE)
    output, err = p.communicate()
    logger.info("Output from delete kafka topic: {0}\nstdout: {1}\nstderr: {2}".format(topic_name, output, err))

### Zopkio specific method that will be run once before all the integration tests.
def setup_suite():

    ## Download and deploy zk and kafka. Configuration for kafka, zookeeper are defined in kafka.json and zookeeper.json.
    _download_components(['zookeeper', 'kafka'])

    _deploy_components(['zookeeper', 'kafka'])

    ## Deploy the three standalone processors. Configurations for them are defined in standalone-processor-{id}.json.
    # _deploy_components(['standalone-processor-1', 'standalone-processor-2', 'standalone-processor-3'])

### Zopkio specific method that will be run once after all the integration tests.
def teardown_suite():
    # Undeploy everything.
    for component in ['kafka', 'zookeeper']:
        deployer = deployers[component]
        for instance, host in c(component + '_hosts').iteritems():
            deployer.undeploy(instance)

def setup():
    ## Create I/O kafka topics.
    _create_kafka_topic('localhost:2181', TEST_INPUT_TOPIC, 3, 1)
    _create_kafka_topic('localhost:2181', TEST_OUTPUT_TOPIC, 3, 1)

    ## Ingest test data into the topic.
    _load_data()

def teardown():
    for topic in [TEST_INPUT_TOPIC, TEST_OUTPUT_TOPIC]:
        logger.info("Deleting topic: {0}.".format(topic))
        _delete_kafka_topic('localhost:2181', topic)