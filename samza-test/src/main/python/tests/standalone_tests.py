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
from kafka import SimpleProducer, SimpleConsumer
import time
import traceback
from standalone_processor import StandaloneProcessor
from zk_client import ZkClient

logger = logging.getLogger(__name__)
NUM_MESSAGES = 50
GROUP_COORDINATION_TIMEOUT_MS = 10
TEST_OUTPUT_TOPIC = 'standaloneIntegrationTestKafkaOutputTopic'
zk_client = None

## TODO: Add docs.
def __validate_output_topic(topic_name, expected_message_count):
    """
    Checks if topic named topic_name has {expected_message_count} messages.
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

## TODO: Add docs.
def __load_data():
    kafka_client = None
    input_topic = 'standaloneIntegrationTestKafkaInputTopic'
    try:
        logger.info("load-data")
        kafka_client = util.get_kafka_client()
        kafka_client.ensure_topic_exists(input_topic)
        producer = SimpleProducer(kafka_client, async=False, req_acks=SimpleProducer.ACK_AFTER_CLUSTER_COMMIT, ack_timeout=30000)
        NUM_MESSAGES = 50
        for message_index in range(1, NUM_MESSAGES + 1):
            logger.info('Publishing message to topic: {0}'.format(input_topic))
            producer.send_messages(input_topic, str(message_index))
    except:
        logger.error(traceback.format_exc(sys.exc_info()))
    finally:
        if kafka_client is not None:
            kafka_client.close()

def __create_processors(deploy_wait_time = 5):
    processors = {}
    for processor_id in ['standalone-processor-1', 'standalone-processor-2', 'standalone-processor-3']:
        processors[processor_id] = StandaloneProcessor(processor_id=processor_id)
        processors[processor_id].deploy(deploy_wait_time = deploy_wait_time)
    return processors

def __kill_all(processors):
    for processor_id, processor in processors.iteritems():
        logger.info("Killing processor: {0}.".format(processor_id))
        processor.kill()

def __setup_zk_client():
    global zk_client
    zk_client = ZkClient('test-app-name', 'test-app-id')
    zk_client.start()

def __teardown_zk_client():
    zk_client.stop()

def test_kill_master():
    try:
        __setup_zk_client()
        processors = __create_processors()
        __load_data()
        leader_processor_id = zk_client.get_leader_processor_id()
        processors.pop(leader_processor_id).kill()

        time.sleep(GROUP_COORDINATION_TIMEOUT_MS)

        job_model = zk_client.get_latest_job_model()

        for processor_id, deployer in processors.iteritems():
            assert processor_id in job_model['containers'], 'Processor id: {0} does not exist in JobModel.'.format(processor_id)
        __kill_all(processors)
    except:
        ## Logging here before raising, since zopkio does not log entire stacktrace.
        logger.error(traceback.format_exc(sys.exc_info()))
        raise
    finally:
        __teardown_zk_client()

def test_kill_single_worker():
    try:
        __setup_zk_client()
        processors = __create_processors()
        __load_data()
        leader_processor_id = zk_client.get_leader_processor_id()
        for processor_id, deployer in processors.iteritems():
            if processor_id != leader_processor_id:
                processors.pop(processor_id).kill()
                break

        time.sleep(GROUP_COORDINATION_TIMEOUT_MS)
        job_model = zk_client.get_latest_job_model()
        for processor_id, deployer in processors.iteritems():
            assert processor_id in job_model['containers'], 'Processor id: {0} does not exist in JobModel.'.format(processor_id)
        __kill_all(processors)
    except:
        ## Logging here before raising, since zopkio does not log entire stacktrace.
        logger.error(traceback.format_exc(sys.exc_info()))
        raise
    finally:
        __teardown_zk_client()

def test_kill_multiple_workers():
    try:
        __setup_zk_client()
        processors = __create_processors()
        __load_data()
        leader_processor_id = zk_client.get_leader_processor_id()
        for processor_id in processors.keys():
            if processor_id != leader_processor_id:
                follower = processors.pop(processor_id)
                follower.kill()

        time.sleep(GROUP_COORDINATION_TIMEOUT_MS)
        job_model = zk_client.get_latest_job_model()
        assert leader_processor_id in job_model['containers'], 'Processor id: {0} does not exist in JobModel.'.format(leader_processor_id)
        __kill_all(processors)
    except:
        ## Logging here before raising, since zopkio does not log entire stacktrace.
        logger.error(traceback.format_exc(sys.exc_info()))
        raise
    finally:
        __teardown_zk_client()

def test_kill_leader_and_follower():
    try:
        __setup_zk_client()
        processors = __create_processors()
        __load_data()
        leader_processor_id = zk_client.get_leader_processor_id()
        processors.pop(leader_processor_id).kill()

        for processor_id in processors.keys():
            processors.pop(processor_id).kill()
            break

        time.sleep(GROUP_COORDINATION_TIMEOUT_MS)
        job_model = zk_client.get_latest_job_model()
        assert leader_processor_id in job_model['containers'], 'Processor id: {0} does not exist in JobModel.'.format(leader_processor_id)
        __kill_all(processors)
    except:
        ## Logging here before raising, since zopkio does not log entire stacktrace.
        logger.error(traceback.format_exc(sys.exc_info()))
        raise
    finally:
        __teardown_zk_client()

def test_pause_resume_master():
    try:
        __setup_zk_client()
        processors = __create_processors()
        __load_data()

        leader_processor_id = zk_client.get_leader_processor_id()
        leader = processors.pop(leader_processor_id)

        logger.info("Pausing the leader processor: {0}.".format(leader_processor_id))
        leader.pause()

        logger.info("Waiting for group coordination timeout: {0}".format(GROUP_COORDINATION_TIMEOUT_MS))
        time.sleep(GROUP_COORDINATION_TIMEOUT_MS)

        job_model = zk_client.get_latest_job_model()
        for processor_id, deployer in processors.iteritems():
            assert processor_id in job_model['containers'], 'Processor id: {0} does not exist in containerModel: {1}.'.format(processor_id, job_model['containers'])

        logger.info("Resuming the leader processor: {0}.".format(leader_processor_id))

        leader.resume()

        logger.info("Waiting for group coordination timeout: {0}".format(GROUP_COORDINATION_TIMEOUT_MS))
        time.sleep(GROUP_COORDINATION_TIMEOUT_MS)

        job_model = zk_client.get_latest_job_model()

        assert leader_processor_id in job_model['containers'], 'Processor id: {0} does not exist in containerModel: {1}.'.format(leader_processor_id, job_model['containers'])

        for processor_id, deployer in processors.iteritems():
            assert processor_id in job_model['containers'], 'Processor id: {0} does not exist in containerModel: {1}.'.format(processor_id, job_model['containers'])

        __kill_all(processors)
    except:
        ## Logging here before raising, since zopkio does not log entire stacktrace.
        logger.error(traceback.format_exc(sys.exc_info()))
        raise
    finally:
        __teardown_zk_client()

def test_pause_master_during_barrier_phase():
    debounce_time_out_ms = 5
    try:
        __setup_zk_client()
        processors = __create_processors(deploy_wait_time=0)
        __load_data()

        leader_processor_id = zk_client.get_leader_processor_id()
        leader = processors.pop(leader_processor_id)

        logger.info("Pausing the leader processor: {0}.".format(leader_processor_id))
        leader.pause()

        logger.info("Waiting for debounce timeout: {0}".format(GROUP_COORDINATION_TIMEOUT_MS))
        time.sleep(debounce_time_out_ms)

        job_model = zk_client.get_latest_job_model()
        for processor_id, deployer in processors.iteritems():
            assert processor_id in job_model['containers'], 'Processor id: {0} does not exist in containerModel: {1}.'.format(processor_id, job_model['containers'])

        logger.info("Resuming the leader processor: {0}.".format(leader_processor_id))

        leader.resume()

        logger.info("Waiting for group coordination timeout: {0}".format(GROUP_COORDINATION_TIMEOUT_MS))
        time.sleep(debounce_time_out_ms)

        job_model = zk_client.get_latest_job_model()

        assert leader_processor_id in job_model['containers'], 'Processor id: {0} does not exist in containerModel: {1}.'.format(leader_processor_id, job_model['containers'])

        for processor_id, deployer in processors.iteritems():
            assert processor_id in job_model['containers'], 'Processor id: {0} does not exist in containerModel: {1}.'.format(processor_id, job_model['containers'])

        __kill_all(processors)
    except:
        ## Logging here before raising, since zopkio does not log entire stacktrace.
        logger.error(traceback.format_exc(sys.exc_info()))
        raise
    finally:
        __teardown_zk_client()

def test_pause_follower_during_barrier_phase():
    debounce_time_out_ms = 5
    try:
        __setup_zk_client()
        processors = __create_processors(deploy_wait_time=0)
        __load_data()

        leader_processor_id = zk_client.get_leader_processor_id()

        follower = None
        for processor_id, deployer in processors.iteritems():
            if processor_id != leader_processor_id:
                follower = processors.pop(processor_id)
                break

        logger.info("Pausing the leader processor: {0}.".format(leader_processor_id))
        follower.pause()

        logger.info("Waiting for debounce timeout: {0}".format(GROUP_COORDINATION_TIMEOUT_MS))
        time.sleep(debounce_time_out_ms)

        job_model = zk_client.get_latest_job_model()
        for processor_id, deployer in processors.iteritems():
            assert processor_id in job_model['containers'], 'Processor id: {0} does not exist in containerModel: {1}.'.format(processor_id, job_model['containers'])

        logger.info("Resuming the leader processor: {0}.".format(leader_processor_id))

        follower.resume()

        logger.info("Waiting for group coordination timeout: {0}".format(GROUP_COORDINATION_TIMEOUT_MS))
        time.sleep(debounce_time_out_ms)

        job_model = zk_client.get_latest_job_model()

        assert follower.get_processor_id() in job_model['containers'], 'Processor id: {0} does not exist in containerModel: {1}.'.format(follower.get_processor_id(), job_model['containers'])

        for processor_id, deployer in processors.iteritems():
            assert processor_id in job_model['containers'], 'Processor id: {0} does not exist in containerModel: {1}.'.format(processor_id, job_model['containers'])

        __kill_all(processors)
    except:
        ## Logging here before raising, since zopkio does not log entire stacktrace.
        logger.error(traceback.format_exc(sys.exc_info()))
        raise
    finally:
        __teardown_zk_client()