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

import stream_registry_python_client.consumer.builder as cbuiler
import unittest


class ConsumerTests(unittest.TestCase):
    registry_config = {'base_url': 'http://streamregistry-test.us-east-1-vpc-88394aef.slb-internal.test.aws.away.black',
                       'region': 'us-east-1-vpc-88394aef',
                       'app_name': 'blahblah'
                       }

    stream_name = 'TestStream'

    def test_simple_consumer(self):
        consumer, topics = cbuiler.create_consumer(registry_config=self.registry_config, stream_name=self.stream_name,
                                                   avro_consumer=False, auto_subscribe=True, kafka_properties={'auto.offset.reset': 'latest'})
        while True:
            msg = consumer.poll(1.0)

            if msg is None:
                continue
            if msg.error():
                print("Consumer error: {}".format(msg.error()))
                break
            print('Received message: {} at offset {}'.format(msg.value().decode('utf-8'), msg.offset()))

        consumer.close()

    def test_avo_consumer(self):
        consumer, topics = cbuiler.create_consumer(registry_config=self.registry_config, stream_name=self.stream_name,
                                                   kafka_properties={'auto.offset.reset': 'latest'})
        while True:
            msg = consumer.poll(1.0)

            if msg is None:
                continue
            if msg.error():
                print("Consumer error: {}".format(msg.error()))
                break
            print('Received message: {} at offset {}'.format(msg.value().decode('utf-8'), msg.offset()))

        consumer.close()
