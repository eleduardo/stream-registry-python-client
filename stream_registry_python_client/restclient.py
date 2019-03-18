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

import requests
from typing import Dict


def register_consumer(registry_config: Dict[str, str], stream_name: str):
    """
    Invoke this function to register a consumer to a stream. Under this cover this function
    invokes the `stream registry api <https://homeaway.github.io/stream-registry/>`_ to
    subscribe the consumer and to receive the subscription metadata which can then be used
    to create the actual consumer.

    :param dict registry_config: Config parameters containing the following
            { 'base_url' : The base URL to the stream service http://streamregistry.org
              'region' : The string that represents the region where a consumer application is running
              'app_name': The name of the application that is consuming
            }
    :param stream_name: The name of the stream to consume from
    :return: a dict object that contains the subscription and configuration binding including the underlying topic names
    """
    __validate_input(registry_config)
    url = registry_config.get("base_url")
    region = registry_config.get("region")
    appname = registry_config.get("app_name")
    if not stream_name:
        logger.error("The name of the stream name is requred")
        raise ValueError("The name of the stream name is requred")

    request_url = "{}/v0/streams/{}/consumers/{}/regions/{}".format(url, stream_name, appname, region)
    response = requests.put(request_url)
    if not response.ok:
        logger.error("Unable to register consumer into the stream registry {} with {}".format(
            response.status_code, response.text))
        return None
    return response.json()


def register_producer(registry_config: Dict[str, str], stream_name: str):
    """
    Invoke this function to register a producer to a stream. Under this cover this function
    invokes the `stream registry api <https://homeaway.github.io/stream-registry/>`_ to
    subscribe the producer and to receive the subscription metadata which can then be used
    to create the actual producer.

    :param dict registry_config: Config parameters containing the following
            { 'base_url' : The base URL to the stream service http://streamregistry.org
              'region' : The string that represents the region where a producing application is running
              'app_name': The name of the application that is producing
            }
    :param stream_name: The name of the stream to produce to
    :return: a dict object that contains the subscription and configuration binding including the underlying topic name
    """
    __validate_input(registry_config)
    url = registry_config.get("base_url")
    region = registry_config.get("region")
    appname = registry_config.get("app_name")

    if not stream_name:
        logger.error("The name of the stream name is requred")
        raise ValueError("The name of the stream name is requred")

    request_url = "{}/v0/streams/{}/producers/{}/regions/{}".format(url, stream_name, appname, region)
    response = requests.put(request_url)
    if not response.ok:
        logger.error("Unable to register consumer into the stream registry {} with {}".format(
            response.status_code, response.text))
        return None
    return response.json()


def __validate_input(registry_config: Dict[str, str]):
    url = registry_config.get("base_url")
    if url is None or not url:
        logger.error("A base_url configuration parameter is required to consume a stream")
        raise ValueError("A base_url configuration parameter is required to consume a stream")
    region = registry_config.get("region")
    if region is None or not region:
        logger.error("The current running region needs to be identified for starting a consumer")
        raise ValueError("The current running region needs to be identified for starting a consumer")
    appname = registry_config.get("app_name")
    if appname is None or not appname:
        logger.error("The name of the consuming application needs to be defined in the configuration")
        raise ValueError("The current running region needs to be identified for starting a consumer")
