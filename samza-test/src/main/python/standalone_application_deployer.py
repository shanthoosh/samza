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
from zopkio.remote_host_helper import better_exec_command, DeploymentError, get_sftp_client, get_ssh_client, open_remote_file, log_output, exec_with_env
import util
import sys
import logging
import zopkio.runtime as runtime
from kafka import SimpleProducer, SimpleConsumer
import struct
import time
import zipfile
import urllib
import traceback
from subprocess import call
from kazoo.client import KazooClient
import zopkio.constants as constants

logger = logging.getLogger(__name__)

##
## TODO: Add class level docs.
##
class StandaloneApplicationDeployer():

    def __init__(self, processor_id, package_id, configs):
        self.username = runtime.get_username()
        self.password = runtime.get_password()
        self.processor_id = processor_id
        self.package_id = package_id
        self.configs = configs
        self.deployer = util.get_deployer(self.processor_id)

    ##
    ## TODO: Add docs.
    ##
    def deploy(self):
        logger.info("Deploying processor with id: {0} and configs: {1}.".format(self.processor_id, self.configs))
        for instance, host in runtime.get_active_config(self.processor_id + '_hosts').iteritems():
            logger.info('Deploying {0} on host: {1}'.format(instance, host))
            self.deployer.deploy(instance, {'hostname': host})

    ##
    ## TODO: Add docs.
    ##
    def stop(self):
        logger.info("Stopping processor with id: {0}.".format(self.package_id))
        for instance, host in runtime.get_active_config(self.processor_id + '_hosts').iteritems():
            logger.info('Stopping {0} on host: {1}'.format(instance, host))
            self.deployer.stop(instance, {'hostname': host})

    ##
    ## TODO: Add docs.
    ##
    def get_processor_id(self):
        return self.processor_id

    ##
    ## TODO: Add docs.
    ##
    def kill(self):
        self.__do_send_signal("kill", "SIGKILL")

    ##
    ## TODO: Add docs.
    ##
    def pause(self):
        self.__do_send_signal("pause", "SIGSTOP")

    ##
    ## TODO: Add docs.
    ##
    def resume(self):
        self.__do_send_signal("resume", "CONT")

    ##
    ## TODO: Add docs.
    ##
    def __do_send_signal(self, command_type, signal):
        command = "kill -{0} {1}".format(signal, self.processor_id)
        result = self.__execute_command(command)
        logger.info("Result of {0} command: {1} is: {2}.".format(command_type, command, result))

    ##
    ## TODO: Add docs.
    ##
    def __get_pid(self, process_name):
        pid_command = "ps aux | grep '{0}' | grep -v grep | tr -s ' ' | cut -d ' ' -f 2 | grep -Eo '[0-9]+'".format(process_name)
        non_failing_command = "{0}; if [ $? -le 1 ]; then true;  else false; fi;".format(pid_command)
        logger.info("Process id command: {0}.".format(pid_command))
        pids = []
        full_output = self.__execute_command(non_failing_command)
        if len(full_output) > 0:
            pids = [int(pid_str) for pid_str in full_output.split('\n') if pid_str.isdigit()]
        return pids

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