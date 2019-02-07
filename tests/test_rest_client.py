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

# -*- coding: utf-8 -*-
import stream_registry_python_client.restclient as restclient
import unittest

from unittest import mock


class TestStreamRegistryClient (unittest.TestCase):

    def test_register_producer_invalid_inputs(self):
        with self.assertRaises(ValueError):
            restclient.register_producer({}, 'foo')
        with self.assertRaises(ValueError):
            restclient.register_producer({'base_url': 'http://localhost'}, 'foo')
        with self.assertRaises(ValueError):
            restclient.register_producer({'base_url': 'http://localhost',
                                          'region': 'someregion'
                                          }, 'foo')
        with self.assertRaises(ValueError):
            restclient.register_producer({'base_url': 'http://localhost',
                                          'region': 'someregion',
                                          'app_name': 'someappname'
                                          }, None)

    @mock.patch('requests.put')
    def test_register_producer_service_down(self, mock_put):
        mock_resp = mock.Mock()
        mock_resp.ok = False
        mock_put.return_value = mock_resp
        producer_data = restclient.register_producer({'base_url': 'http://localhost',
                                      'region': 'someregion',
                                      'app_name': 'someappname'
                                      }, 'testapp')
        self.assertIsNone(producer_data)