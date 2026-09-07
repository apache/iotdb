# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
import sys
import types
import unittest
from importlib import import_module


class TEndPoint:
    def __init__(self, ip=None, port=None):
        self.ip = ip
        self.port = port


thrift_module = types.ModuleType("iotdb.thrift")
common_module = types.ModuleType("iotdb.thrift.common")
ttypes_module = types.ModuleType("iotdb.thrift.common.ttypes")
ttypes_module.TEndPoint = TEndPoint
sys.modules.setdefault("iotdb.thrift", thrift_module)
sys.modules.setdefault("iotdb.thrift.common", common_module)
sys.modules.setdefault("iotdb.thrift.common.ttypes", ttypes_module)

config_module = import_module("iotdb.ainode.core.config")
exception_module = import_module("iotdb.ainode.core.exception")
parse_endpoint_url = config_module.parse_endpoint_url
BadNodeUrlException = exception_module.BadNodeUrlException


class ParseEndpointUrlTest(unittest.TestCase):
    def test_parse_bracketed_ipv6_endpoint(self):
        endpoint = parse_endpoint_url("[::1]:6667")

        self.assertEqual("::1", endpoint.ip)
        self.assertEqual(6667, endpoint.port)

    def test_parse_legacy_ipv6_endpoint(self):
        endpoint = parse_endpoint_url("::1:6667")

        self.assertEqual("::1", endpoint.ip)
        self.assertEqual(6667, endpoint.port)

    def test_reject_malformed_endpoint(self):
        malformed_urls = [
            "[]:123",
            "foo[::1]:6667",
            "foo]bar:6667",
        ]

        for endpoint_url in malformed_urls:
            with self.subTest(endpoint_url=endpoint_url):
                with self.assertRaises(BadNodeUrlException):
                    parse_endpoint_url(endpoint_url)


if __name__ == "__main__":
    unittest.main()
