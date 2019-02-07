# Copyright (c) 2018 Expedia Group.
# All rights reserved.  http://www.homeaway.com
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#       http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from logzero import logger
from typing import Dict
from confluent_kafka import Consumer
from confluent_kafka.avro import AvroConsumer

import stream_registry_python_client.restclient as client

__all__ = ["create_consumer"]


def create_consumer(registry_config: Dict[str, str], stream_name: str, kafka_properties: Dict[str, str] = None,
                    avro_consumer: bool = True, auto_subscribe: bool = True):
    """
    Creates a High level kafka consumer for the desired stream. This method will talk to the stream registry to
    register the consumer based on the registry configuration passed and will optionally auto subscribe the consumer
    to all the topics that represent the stream

    :param dict registry_config: Config parameters containing the following
            { 'base_url' : The base URL to the stream service http://streamregistry.org
              'region' : The string that represents the region where a producing application is running
              'app_name': The name of the application that is producing
            }
    :param stream_name: The name of the stream to consume.
    :param kafka_properties: any kafka consumer properties which will be merged with the default from the stream
                             registry. One example would be 'auto.offset.reset': 'latest', etc. These need to be
                             valid Kafka configuration properties.
    :param avro_consumer: If True (default) an AVRO consumer will be created
    :param auto_subscribe: if True (default) the consumer.subscribe method will be invoked for all the topics that
                           represent the stream.
    :return: a tuple of the consumer and an array of topics. If `auto_subscribe` was set to True (default) the consumer
            would be subscribed already and is ready to start consuming/polling.
    """
    registration = client.register_consumer(registry_config, stream_name)
    if registration is None:
        logger.error("Unable to create Kafka Consumer since the stream registry did not respond")
        return None

    """Traverse the JSON object to get to the actual kafka configuration"""
    config_element = registration['regionStreamConfigList'][0]['streamConfiguration']

    """ Merge the configuration """
    properties = __merge_properties(config_element, kafka_properties)

    if 'group.id' not in properties:
        app_name = registry_config['app_name']
        logger.info(
            "A group id for the consumer was not specified, adding it as the application name: {}".format(app_name))
        properties['group.id'] = app_name

    if avro_consumer:
        c = __build_avro_consumer(properties)
    else:
        c = __build_highlevel_consumer(properties)

    topics = registration['regionStreamConfigList'][0]['topics']
    if auto_subscribe:
        c.subscribe(topics)
    return c, topics


def __build_highlevel_consumer(kafka_config: Dict[str, str]):
    """for reasons only understood by confluent, an unknown key is not ignored, it makes the building fail.. so if not doing
       avro, remove the registry..."""
    del kafka_config['schema.registry.url']
    return Consumer(kafka_config)


def __build_avro_consumer(kafka_config: Dict[str, str]):
    return AvroConsumer(kafka_config)

def __merge_properties(stream_registry_props: Dict[str, str], user_properties: Dict[str, str]):
    """ Merge stream registry configuration into kafka properties"""
    properties = {}
    if user_properties is not None:
        for k, v in user_properties.items():
            properties[k] = v
    if stream_registry_props is not None:
        for k, v in stream_registry_props.items():
            properties[k] = v
    return properties