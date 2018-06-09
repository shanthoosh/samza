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
from zopkio.remote_host_helper import better_exec_command, DeploymentError, get_sftp_client, get_ssh_client, open_remote_file, log_output, exec_with_env
import logging
import zopkio.runtime as runtime
import time
import zopkio.adhoc_deployer as adhoc_deployer

logger = logging.getLogger(__name__)

## TODO: Add class level docs.
class StandaloneProcessor:

    def __init__(self, processor_id):
        self.username = runtime.get_username()
        self.password = runtime.get_password()
        self.processor_id = processor_id
        self.deployer = adhoc_deployer.SSHDeployer(self.processor_id, {
            'install_path': os.path.join(runtime.get_active_config('remote_install_path'), runtime.get_active_config(self.processor_id + '_install_path')),
            'executable': runtime.get_active_config(self.processor_id + '_executable'),
            'post_install_cmds': runtime.get_active_config(self.processor_id + '_post_install_cmds', []),
            'start_command': runtime.get_active_config(self.processor_id + '_start_cmd'),
            'stop_command': runtime.get_active_config(self.processor_id + '_stop_cmd'),
            'extract': True,
            'sync': True,
        })

    ## TODO: Add docs.
    def deploy(self):
        logger.info("Deploying processor with id: {0}.".format(self.processor_id))
        for instance, host in runtime.get_active_config(self.processor_id + '_hosts').iteritems():
            logger.info('Deploying {0} on host: {1}'.format(instance, host))
            self.deployer.deploy(instance, {'hostname': host})
            time.sleep(5)

    ## TODO: Add docs.
    def stop(self):
        logger.info("Stopping processor with id: {0}.".format(self.processor_id))
        for instance, host in runtime.get_active_config(self.processor_id + '_hosts').iteritems():
            logger.info('Stopping {0} on host: {1}'.format(instance, host))
            self.deployer.stop(instance, {'hostname': host})

    ## TODO: Add docs.
    def get_processor_id(self):
        return self.processor_id

    ## TODO: Add docs.
    def kill(self):
        self.__send_signal_to_processor("kill", "SIGKILL")

    ## TODO: Add docs.
    def pause(self):
        self.__send_signal_to_processor("pause", "SIGSTOP")

    ## TODO: Add docs.
    def resume(self):
        self.__send_signal_to_processor("resume", "CONT")

    ## TODO: Add docs.
    def __send_signal_to_processor(self, command_type, signal):
        linux_process_pids = self.__get_pid(self.processor_id)
        for linux_process_pid in linux_process_pids:
            command = "kill -{0} {1}".format(signal, linux_process_pid)
            result = self.__execute_command(command)
            logger.info("Result of {0} command: {1} is: {2}.".format(command_type, command, result))

    ## TODO: Add docs.
    def __get_pid(self, process_name):
        ps_command = "ps aux | grep '{0}' | grep -v grep | tr -s ' ' | cut -d ' ' -f 2 | grep -Eo '[0-9]+'".format(process_name)
        non_failing_command = "{0}; if [ $? -le 1 ]; then true;  else false; fi;".format(ps_command)
        logger.info("Executing command: {0}.".format(non_failing_command))
        full_output = self.__execute_command(non_failing_command)
        pids = []
        if len(full_output) > 0:
            pids = [int(pid_str) for pid_str in full_output.split('\n') if pid_str.isdigit()]
        return pids

    ## TODO: Add docs.
    def __execute_command(self, command):
        with get_ssh_client('localhost', username=self.username, password=self.password) as ssh:
            chan = exec_with_env(ssh, command, msg="Failed to get PID", env={})
        execution_result = ''
        while True:
            result_buffer = chan.recv(block_size=16)
            if len(result_buffer) == 0:
                break
            execution_result += result_buffer
        return execution_result