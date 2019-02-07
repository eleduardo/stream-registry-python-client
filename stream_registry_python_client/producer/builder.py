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
from confluent_kafka import Producer
from confluent_kafka import avro
from confluent_kafka.avro import AvroProducer

import stream_registry_python_client.restclient as client

__all__ = ['create_producer']


def create_producer(registry_config: Dict[str, str], stream_name: str, kafka_properties=None):
    """
    Call this method to create a Kafka High Level producer. This method will subscribe the producer with the stream
    registry and initialize the producer object with the properties coming from the registry.

    :param dict registry_config: Config parameters containing the following
            { 'base_url' : The base URL to the stream service http://streamregistry.org
              'region' : The string that represents the region where a producing application is running
              'app_name': The name of the application that is producing
            }
    :param stream_name: The name of the stream to produce to.
    :param kafka_properties: any kafka producer properties which will be merged with the default from the stream
                             registry. These need to be valid Kafka configuration properties.
    :return: a tuple with the producer object and the topic for the stream
    """
    registration = client.register_producer(registry_config, stream_name)
    if registration is None:
        logger.error("Unable to create Kafka Consumer since the stream registry did not respond")
        return None, None

    """Traverse the JSON object to get to the actual kafka configuration"""
    config_elements = registration['regionStreamConfigList'][0]['streamConfiguration']
    """ Merge stream registry configuration into kafka properties"""
    properties = __merge_properties(config_elements, kafka_properties)

    """ For whatever reason a unknow property is not ignored so for now remove registry since it is not needed"""
    properties.pop('schema.registry.url')

    p = Producer(properties)
    topic = registration['regionStreamConfigList'][0]['topics'][0]
    return p, topic


def create_avro_producer(registry_config: Dict[str, str], stream_name: str, key_schema_str: str, value_schema_str: str,
                         kafka_properties: Dict[str, str] = None):
    """
    Call this method to create a Kafka AVRO ready producer. This method will subscribe the producer with the stream
    registry and initialize the producer object with the properties coming from the registry.

    :param dict registry_config: Config parameters containing the following
            { 'base_url' : The base URL to the stream service http://streamregistry.org
              'region' : The string that represents the region where a producing application is running
              'app_name': The name of the application that is producing
            }
    :param stream_name: The name of the stream to produce to.
    :param key_schema_str: The string that represents the avro schema (AVSC) to the key of the message. Since AVRO needs
                           a schema to parse the encoded objects this CANNOT be None and MUST be provided.
    :param value_schema_str: The string that represents the avro schema (AVSC) to the value of the message. Since AVRO
                            needs a schema to parse the encoded objects this CANNOT be None and MUST be provided.
    :param kafka_properties: any kafka producer properties which will be merged with the default from the stream
                             registry. These need to be valid Kafka configuration properties.
    :return: a tuple with the producer object and the topic for the stream
    """
    if value_schema_str is None or key_schema_str is None:
        logger.error("An Avro schema is required for key and value")
        return None

    key_schema= avro.loads(key_schema_str)

    value_schema = avro.loads(value_schema_str)
    logger.info("Properly initalized AVRO schema objects for a producer")

    registration = client.register_producer(registry_config, stream_name)
    if registration is None:
        logger.error("Unable to create Kafka Consumer since the stream registry did not respond")
        return None

    """Traverse the JSON object to get to the actual kafka configuration"""
    config_elements = registration['regionStreamConfigList'][0]['streamConfiguration']
    """ Merge stream registry configuration into kafka properties"""
    properties = __merge_properties(config_elements, kafka_properties)

    # build the avro producer bound to the schema that was passed
    p = AvroProducer(properties, default_key_schema=key_schema, default_value_schema=value_schema)
    topic = registration['regionStreamConfigList'][0]['topics'][0]
    return p, topic


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
