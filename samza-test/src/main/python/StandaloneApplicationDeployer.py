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
import re
import logging
import json
import requests
import shutil
import tarfile
import zopkio.constants as constants
import zopkio.runtime as runtime
import templates

from subprocess import PIPE, Popen
from zopkio.deployer import Deployer, Process
from zopkio.remote_host_helper import better_exec_command, DeploymentError, get_sftp_client, get_ssh_client, open_remote_file
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

logger = logging.getLogger(__name__)

##
## TODO: Add class level docs.
##
class StandaloneApplicationDeployer(Deployer):

    def __init__(self, processor_id, package_id, configs):
        logging.getLogger("paramiko").setLevel(logging.ERROR)
        self.username = runtime.get_username()
        self.password = runtime.get_password()
        self.processor_id = processor_id
        self.package_id = package_id
        self.configs = configs
        Deployer.__init__(self)

    ##
    ## TODO: Add docs.
    ##
    def deploy(self):
        logger.info("Deploying processor with id: {0} and configs: {1}.".format(self.processor_id, self.configs))
        deployer = util.get_deployer(self.processor_id)
        for instance, host in runtime.get_active_config(self.processor_id + '_hosts').iteritems():
            logger.info('Deploying {0} on host: {1}'.format(instance, host))
            deployer.deploy(instance, {'hostname': host})

    ##
    ## TODO: Add docs.
    ##
    def stop(self):
        logger.info("Stopping processor with id: {0}.".format(self.package_id))
        deployer = util.get_deployer(self.processor_id)
        for instance, host in runtime.get_active_config(self.processor_id + '_hosts').iteritems():
            logger.info('Stopping {0} on host: {1}'.format(instance, host))
            deployer.stop(instance, {'hostname': host})

    ##
    ## TODO: Add docs.
    ##
    def get_processor_id(self):
        return self.processor_id

    ##
    ## TODO: Add docs.
    ##
    def kill(self):
        kill_command = "kill -9 {0}".format(self.processor_id)
        result = __execute_command(kill_command)
        logger.info("Result of kill command: {0} is: {1}.".format(kill_command, result))

    ##
    ## TODO: Add docs.
    ##
    def pause(self):
        pause_command = "kill -SIGSTOP {0}".format(self.processor_id)
        result = __execute_command(pause_command)
        logger.info("Result of pause command: {0} is: {1}.".format(pause_command, result))

    ##
    ## TODO: Add docs.
    ##
    def resume(self):
        resume_command = "kill -CONT {0}".format(self.processor_id)
        result = __execute_command(resume_command)
        logger.info("Result of resume command: {0} is: {1}.".format(resume_command, result))

    ##
    ## TODO: Add docs.
    ##
    def __execute_command(self, command):
        RECV_BLOCK_SIZE = 16
        HOST_NAME = 'localhost'
        with get_ssh_client(HOST_NAME, username=self.username, password=self.password) as ssh:
            chan = exec_with_env(ssh, command, msg="Failed to get PID", env={})
        output = chan.recv(RECV_BLOCK_SIZE)
        full_output = output
        while len(output) > 0:
            output = chan.recv(RECV_BLOCK_SIZE)
            full_output += output
        return full_output

    ##
    ## TODO: Add docs.
    ##
    def __get_pid(self, process_name):
        pid_command = "ps aux | grep '{0}' | grep -v grep | tr -s ' ' | cut -d ' ' -f 2 | grep -Eo '[0-9]+'".format(process_name)
        non_failing_command = "{0}; if [ $? -le 1 ]; then true;  else false; fi;".format(pid_command)
        logger.info("Process id command: {0}.".format(pid_command))
        pids = []
        full_output = __execute_command(self, non_failing_command)
        if len(full_output) > 0:
            pids = [int(pid_str) for pid_str in full_output.split('\n') if pid_str.isdigit()]
        return pids


    def _validate_configs(self, configs, config_keys):
        for required_config in config_keys:
            assert configs.get(required_config), 'Required config is undefined: {0}'.format(required_config)

    def _get_merged_configs(self, configs):
        tmp = self.default_configs.copy()
        tmp.update(configs)
        return tmp

    def get_package_tgz_name(self, package_id):
        return '{0}.tgz'.format(package_id)