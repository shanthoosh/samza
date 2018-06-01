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

logger = logging.getLogger(__name__)

class StandaloneApplicationDeployer(Deployer):
    def __init__(self, processor_ids = [], configs={}):
        logging.getLogger("paramiko").setLevel(logging.ERROR)
        # map from job_id to app_id
        self.username = runtime.get_username()
        self.password = runtime.get_password()
        self.processor_ids = processor_ids
        self.processor_id_to_process_id_map = {}
        self.default_configs = configs
        Deployer.__init__(self)

    def install(self, package_id, configs={}):
        self.processor_ids = []

    def start(self, processor_ids, configs={}):
        self.processor_ids = []

    def get_processor_ids(self):
        return self.processor_ids

    def pause(self, processor_id, configs={}):
        process_id = self.processor_id_to_process_id_map[processor_id]
        exec "KILL -TSTP {0}".format(processor_id)

    def resume(self, processor_id, configs={}):
        process_id = self.processor_id_to_process_id_map[processor_id]
        exec "KILL -CONT {0}".format(processor_id)

    def kill(self, processor_id, configs={}):
        process_id = self.processor_id_to_process_id_map[processor_id]
        exec "KILL -9 {0}".format(processor_id)

    def _validate_configs(self, configs, config_keys):
        for required_config in config_keys:
            assert configs.get(required_config), 'Required config is undefined: {0}'.format(required_config)

    def _get_merged_configs(self, configs):
        tmp = self.default_configs.copy()
        tmp.update(configs)
        return tmp

    def _get_package_tgz_name(self, package_id):
        return '{0}.tgz'.format(package_id)