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

import time
from os import environ

from testcontainers.core.container import DockerContainer
from testcontainers.core.exceptions import ContainerStartException
from testcontainers.core.utils import setup_logger
from testcontainers.core.waiting_utils import wait_container_is_ready

from iotdb.Session import Session

logger = setup_logger(__name__)


class IoTDBContainer(DockerContainer):
    IOTDB_USER = environ.get("IOTDB_USER", "root")
    IOTDB_PASSWORD = environ.get("IOTDB_PASSWORD", "root")

    def _configure(self):
        pass

    @wait_container_is_ready()
    def _connect(self):
        session = Session(
            self.get_container_host_ip(), self.get_exposed_port(6667), "root", "root"
        )
        session.open(False)
        with session.execute_statement("SHOW CLUSTER") as session_data_set:
            while session_data_set.has_next():
                if (
                    session_data_set.next().get_fields()[2].get_string_value()
                    != "Running"
                ):
                    raise ContainerStartException("IoTDB is not started")
        session.close()

    def __init__(self, image="apache/iotdb:latest", **kwargs):
        super(IoTDBContainer, self).__init__(image)
        self.port_to_expose = 6667
        self.with_exposed_ports(self.port_to_expose)

    def start(self):
        self._configure()
        super().start()
        self._connect()
        # sleep 2s to make sure the confignode cached the datanode info
        time.sleep(2)
        return self

    def stop(self, force=True, delete_volume=True):
        logger.info(self.get_wrapped_container().logs().decode("utf-8"))
        super().stop(force, delete_volume)
        return self
