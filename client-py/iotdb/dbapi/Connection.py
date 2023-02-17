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

import logging

from iotdb.Session import Session

from .Cursor import Cursor
from .Exceptions import ConnectionError, ProgrammingError

logger = logging.getLogger("IoTDB")


class Connection(object):
    def __init__(
        self,
        host,
        port,
        username=Session.DEFAULT_USER,
        password=Session.DEFAULT_PASSWORD,
        fetch_size=Session.DEFAULT_FETCH_SIZE,
        zone_id=Session.DEFAULT_ZONE_ID,
        enable_rpc_compression=False,
        sqlalchemy_mode=False,
    ):
        self.__session = Session(host, port, username, password, fetch_size, zone_id)
        self.__sqlalchemy_mode = sqlalchemy_mode
        self.__is_close = True
        try:
            self.__session.open(enable_rpc_compression)
            self.__is_close = False
        except Exception as e:
            raise ConnectionError(e)

    def close(self):
        """
        Close the connection now
        """
        if self.__is_close:
            return
        self.__session.close()
        self.__is_close = True

    def cursor(self):
        """
        Return a new Cursor Object using the connection.
        """
        if not self.__is_close:
            return Cursor(self, self.__session, self.__sqlalchemy_mode)
        else:
            raise ProgrammingError("Connection closed")

    def commit(self):
        """
        Not supported method.
        """
        pass

    def rollback(self):
        """
        Not supported method.
        """
        pass

    @property
    def is_close(self):
        """
        This read-only attribute specified whether the object is closed
        """
        return self.__is_close

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
