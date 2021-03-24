from os import environ

from testcontainers.core.container import DockerContainer
from testcontainers.core.waiting_utils import wait_container_is_ready

from iotdb.Session import Session


class IoTDBContainer(DockerContainer):
    IOTDB_USER = environ.get("IOTDB_USER", "root")
    IOTDB_PASSWORD = environ.get("IOTDB_PASSWORD", "root")

    def _configure(self):
        pass

    @wait_container_is_ready()
    def _connect(self):
        session = Session(self.get_container_host_ip(), self.get_exposed_port(6667), 'root', 'root')
        session.open(False)
        session.close()

    def __init__(self, image="apache/iotdb:latest", **kwargs):
        super(IoTDBContainer, self).__init__(image)
        self.port_to_expose = 6667
        self.with_exposed_ports(self.port_to_expose)

    def start(self):
        self._configure()
        super().start()
        self._connect()
        return self