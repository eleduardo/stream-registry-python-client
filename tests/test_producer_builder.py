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

import stream_registry_python_client.producer.builder as pbuilder
import unittest


class ProducerTests(unittest.TestCase):
    registry_config = {'base_url': 'http://streamregistry',
                       'region': 'us-east',
                       'app_name': 'pythontest'
                       }

    stream_name = 'TestStream'

    def test_simple_producer(self):
        p, topic = pbuilder.create_producer(self.registry_config, self.stream_name)
        p.poll(0)
        p.produce(topic, 'helloworld this is python'.encode('utf-8'), callback=self.callback)
        p.flush()

    def test_avro_producer(self):
        key_schema = """
            {"type":"string"}
        """
        value_schema = """
            {"type":"record","name":"MyCoolDomainEvent","namespace":"com.homeaway.stream.fake","doc":"* Schema for MyCoolDomainEvents","fields":[{"name":"header","type":{"type":"record","name":"EventHeader","namespace":"com.homeaway.streamingplatform","doc":"* Header that all stream events must have; There must be a non-optional `header` field on all stream schemas with this type","fields":[{"name":"time","type":"long","doc":"Milliseconds since epoch that this event occurred"},{"name":"threadId","type":["null","string"],"doc":"ThreadId of the event","default":null},{"name":"requestMarker","type":["null","string"],"doc":"Request marker for this request chain; Opaque string","default":null},{"name":"environment","type":["null","string"],"doc":"Environment in which this event was generated\\n    Must be one of these:\\n        DEV, TEST, STAGE, PRODUCTION","default":null},{"name":"server","type":["null","string"],"doc":"The host for this event (using LinkedIn's decoder/encoder fieldnames)","default":null},{"name":"service","type":["null","string"],"doc":"The application for this event  (using LinkedIn's decoder/encoder fieldnames)","default":null},{"name":"serviceVersion","type":["null","string"],"doc":"Version of the application which generated this event","default":null},{"name":"region","type":["null","string"],"doc":"Region in which this event was generated; For Multi-Paas, this is the multipaas region; For elmer, this is the elmer\\n    environment (usprd1, apiprd1, usstg1, ustst1, etc.)"},{"name":"sessionId","type":["null","string"],"doc":"The sessionId of this event","default":null},{"name":"visitorId","type":["null","string"],"doc":"The visitorId of this event","default":null},{"name":"deviceId","type":["null","string"],"doc":"The deviceId of this event","default":null},{"name":"location","type":["null",{"type":"map","values":"string"}],"doc":"Location from which this request was made\\n    Expected fields:\\n        city=private, region=***, country=**, lat=0.000, lng=0.000","default":null},{"name":"traceId","type":["null","string"],"doc":"Trace ID for this request chain; Opaque String","default":null},{"name":"deviceClassification","type":"int","doc":"* Ordinal indicating bot classification.\\n   * See https://github.homeawaycorp.com/cloud/bots/blob/master/docs/living-with-bots.md#classification-scale\\n  *","default":0}],"aliases":["com.homeaway.commons.logging.events.EventHeader"]},"doc":"* Standard event header; This must be included, and must be named 'header'."}]}
        """
        event_object = {
            "header": {
                "time": 1545092723502,
                "threadId": None,
                "requestMarker": None,
                "environment": None,
                "server": "localhost",
                "service": "mock",
                "serviceVersion": "1.0.0",
                "region": "test",
                "sessionId": None,
                "visitorId": None,
                "deviceId": None,
                "location": None,
                "traceId": None,
                "deviceClassification": 0
            }
        }
        p, topic = pbuilder.create_avro_producer(self.registry_config, stream_name=self.stream_name,
                                                 key_schema_str=key_schema, value_schema_str=value_schema)
        p.poll(0)
        p.produce(topic=topic, key='2756ba06-ccac-4ce7-b70f-c1beac0ef4dc', value=event_object, callback=self.callback)
        p.flush()

    def callback(self, err, msg):
        if err is not None:
            print('Message delivery failed: {}'.format(err))
        else:
            print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))
