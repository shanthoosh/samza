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

logger = logging.getLogger(__name__)
deployers = {}

### Download all the components.
def _download_components():
    for url_key in ['url_kafka', 'url_zookeeper']:
        logger.info('Getting download URL for: {0}'.format(url_key))
        url = c(url_key)
        filename = os.path.basename(url)
        if os.path.exists(filename):
            logger.debug('Using cached file: {0}'.format(filename))
        else:
            logger.info('Downloading: {0}'.format(url))
            urllib.urlretrieve(url, filename)

###
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

            time.sleep(5)

## zookeeper_servers: Comma seperated list of zookeeper servers of the form host:port.
## topic_name: kafka topic to be created.
## partition_count: Number of partitions of the kafka topic.
## replication_factor: Replication factor of the kafka topic.
def _create_kafka_topic(zookeeper_servers, topic_name, partition_count, replication_factor):

    ### Using command line utility to create kafka topic since kafka python API doesn't support configuring partitionCount during topic creation.

    base_dir = os.getcwd()
    logger.info('Current working directory: {0}'.format(base_dir))

    create_topic_command = 'sh {0}/deploy/kafka/kafka_2.10-0.10.1.1/bin/kafka-topics.sh --create --zookeeper {1} --replication-factor {2} --partitions {3} --topic {4}'.format(base_dir, zookeeper_servers, replication_factor, partition_count, topic_name)
    logger.info("running command")
    logger.info(create_topic_command)
    p = Popen(create_topic_command, stdin=PIPE, stdout=PIPE, stderr=PIPE)
    output, err = p.communicate()
    logger.info("Output from kafka-topics.sh:\nstdout: {0}\nstderr: {1}".format(output, err))

### Zopkio specific method that will be run before all the integration test once.
def setup_suite():
    _download_components()

    _deploy_components(['zookeeper', 'kafka'])

    _create_kafka_topic('localhost:2181','standaloneIntegrationTestKafkaInputTopic', 3, 1)

    _deploy_components(['standalone-processor-1', 'standalone-processor-2', 'standalone-processor-3'])

### Zopkio specific method that will be run after all the integration test once.
def teardown_suite():
    # stream_application_deployer.uninstall('tests')

    # Undeploy everything.
    for name, deployer in deployers.iteritems():
        for instance, host in c(name + '_hosts').iteritems():
            deployer.undeploy(instance)
