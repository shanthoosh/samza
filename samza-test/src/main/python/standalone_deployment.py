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
import shutil
import urllib
import zopkio.runtime as runtime
import zopkio.adhoc_deployer as adhoc_deployer
from zopkio.runtime import get_active_config as c
from subprocess import PIPE, Popen

import time

logger = logging.getLogger(__name__)
deployers = None

def _download_components():
    for url_key in ['url_kafka', 'url_zookeeper']:
        logger.debug('Getting download URL for: {0}'.format(url_key))
        url = c(url_key)
        filename = os.path.basename(url)
        if os.path.exists(filename):
            logger.debug('Using cached file: {0}'.format(filename))
        else:
            logger.info('Downloading: {0}'.format(url))
            urllib.urlretrieve(url, filename)

def _new_ssh_deployer(config_prefix, name=None):
    deployer_name = config_prefix if name == None else name
    return adhoc_deployer.SSHDeployer(deployer_name, {
        'install_path': os.path.join(c('remote_install_path'), c(config_prefix + '_install_path')),
        'executable': c(config_prefix + '_executable'),
        'post_install_cmds': c(config_prefix + '_post_install_cmds', []),
        'start_command': c(config_prefix + '_start_cmd'),
        'stop_command': c(config_prefix + '_stop_cmd'),
        'extract': True,
        'sync': True,
    })

def _deploy_components(deployers, components):
    for component in components:
        deployer = deployers[component]
        runtime.set_deployer(component, deployer)
        for instance, host in c(component + '_hosts').iteritems():
            logger.info('Deploying {0} on host: {1}'.format(instance, host))
            deployer.deploy(instance, {
                 'hostname': host
            })

            time.sleep(5)

## zookeeper_servers: Comma seperated list of zookeeper server of form host:port.
## topic_name: Name of kafka topic to create.
## partition_count: Number of partitions of kafka topic to be created.
## replication_factor: Replication factor property of the kafka topic.
def _create_kafka_topic(zookeeper_servers, topic_name, partition_count, replication_factor):
    ### Employ command line to create kafka topic since existing kafka python API doesn't allow to configure partition count when creating topic.
    base_dir = os.getcwd()
    logger.info('Current working directory: {0}'.format(base_dir))

    command='sh {0}/deploy/kafka/kafka_2.10-0.10.1.1/bin/kafka-topics.sh --create --zookeeper {1} --replication-factor {2} --partitions {3} --topic {4}'.format(base_dir, zookeeper_servers, replication_factor, partition_count, topic_name)
    logger.info("running command")
    logger.info(command)
    p = Popen(command, stdin=PIPE, stdout=PIPE, stderr=PIPE)
    output, err = p.communicate()
    logger.info("Output from kafka-topics.sh:\nstdout: {0}\nstderr: {1}".format(output, err))

def setup_suite():
    global deployers

    deployers = {
        'zookeeper': _new_ssh_deployer('zookeeper'),
        'kafka': _new_ssh_deployer('kafka'),
        'standalone-processor-1': _new_ssh_deployer('standalone-processor-1'),
        'standalone-processor-2': _new_ssh_deployer('standalone-processor-2'),
        'standalone-processor-3': _new_ssh_deployer('standalone-processor-3'),
    }

    _download_components()

    _deploy_components(deployers, ['zookeeper', 'kafka'])

    _create_kafka_topic('localhost:2181','standaloneIntegrationTestKafkaInputTopic', 3, 1)

    _deploy_components(deployers, ['standalone-processor-1', 'standalone-processor-2', 'standalone-processor-3'])

def teardown_suite():
    # stream_application_deployer.uninstall('tests')

    # Undeploy everything.
    for name, deployer in deployers.iteritems():
        for instance, host in c(name + '_hosts').iteritems():
            deployer.undeploy(instance)
