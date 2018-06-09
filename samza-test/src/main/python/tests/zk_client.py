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

import logging
from kazoo.client import KazooClient
import json

logger = logging.getLogger(__name__)

## TODO: Add docs.
class ZkClient:

    ## TODO: Add docs.
    def __init__(self, app_name, app_id):
        self.zk_client = KazooClient(hosts='127.0.0.1:2181')
        self.zk_base_dir = 'app-{0}-{1}/{2}-{3}-coordinationData'.format(app_name, app_id, app_name, app_id)

    ## TODO: Add docs.
    def start(self):
        self.zk_client.start()

    ## TODO: Add docs.
    def stop(self):
        self.zk_client.stop()

    ## TODO: Add docs.
    def get_job_model_version(self):
        try:
            job_model_version_path = '{0}/JobModelGeneration/jobModelVersion'.format(self.zk_base_dir)
            job_model_version, stat = self.zk_client.get(job_model_version_path)
            logger.info('Retrieved job model version: {0}, stat.version: {1}.'.format(job_model_version, stat.version))
            return stat.version
        except:
            logger.error(traceback.format_exc(sys.exc_info()))

    ## TODO: Add docs.
    def get_latest_job_model(self):
        try:
            childZkNodes = self.zk_client.get_children('{0}/JobModelGeneration/jobModels/'.format(self.zk_base_dir))
            childZkNodes.sort()
            childZkNodes.reverse()

            job_model_generation_path = '{0}/JobModelGeneration/jobModels/{1}/'.format(self.zk_base_dir, childZkNodes[0])
            job_model, _ = self.zk_client.get(job_model_generation_path)

            """ 
            ZkClient java library persist data in the following format in zookeeper:
                    class_name, data_length, actual_data
    
            JobModel json manipulation: Delete all the characters before first occurrence of '{' in jobModel json string.

            Primitive json deserialization fails without the above custom string massaging. This will be removed after SAMZA-1876.
            """

            first_curly_index = job_model.find('{')
            job_model = job_model[first_curly_index: ]
            job_model_dict = json.loads(job_model)
            return job_model_dict
        except:
            logger.error(traceback.format_exc(sys.exc_info()))

    ## TODO: Add docs.
    def get_all_processors(self):
        try:
            logger.info("Executing get_leader_processor_id: {0}.".format(self.zk_base_dir))
            processors_path =  '{0}/processors'.format(self.zk_base_dir)
            childZkNodes = self.zk_client.get_children(processors_path)
            childZkNodes.sort()

            processor_ids = []
            for childZkNode in childZkNodes:
                child_processor_path = '{0}/{1}'.format(processors_path, childZkNode)
                processor_data, _ = self.zk_client.get(child_processor_path)
                host, processor_id = processor_data.split(" ")
                processor_ids.append(processor_id)

            if len(processor_ids) > 0:
                return processor_ids[0]
            else:
                return None
        except:
            logger.error(traceback.format_exc(sys.exc_info()))

    ## TODO: Add docs.
    def get_leader_processor_id(self):
        try:
            processors_path =  '{0}/processors'.format(self.zk_base_dir)
            childZkNodes = self.zk_client.get_children(processors_path)
            childZkNodes.sort()
            child_processor_path = '{0}/{1}'.format(processors_path, childZkNodes[0])
            processor_data, _ = self.zk_client.get(child_processor_path)
            host, processor_id = processor_data.split(" ")
            return processor_id
        except:
            logger.error(traceback.format_exc(sys.exc_info()))

