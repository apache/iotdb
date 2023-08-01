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

from os import environ

from testcontainers.core.container import DockerContainer
from testcontainers.core.utils import setup_logger
from testcontainers.core.waiting_utils import wait_container_is_ready, wait_for_logs

logger = setup_logger(__name__)


class IoTDBContainer(DockerContainer):
    IOTDB_USER = environ.get("IOTDB_USER", "root")
    IOTDB_PASSWORD = environ.get("IOTDB_PASSWORD", "root")

    def _configure(self):
        pass

    @wait_container_is_ready()
    def __connect(self):
        wait_for_logs(self, "Now, enjoy yourself!", 60)

    def __init__(self, image="apache/iotdb:latest", **kwargs):
        super(IoTDBContainer, self).__init__(image)
        self.port_to_expose = 6667
        self.with_exposed_ports(self.port_to_expose)

    def start(self):
        self._configure()
        super().start()
        return self

    def stop(self, force=True, delete_volume=True):
        super().stop(force, delete_volume)
        logger.info(self.get_wrapped_container().logs())
        return self
