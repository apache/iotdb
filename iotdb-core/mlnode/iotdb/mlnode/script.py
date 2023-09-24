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
import os
import platform
import sys

from iotdb.mlnode.config import descriptor
from iotdb.mlnode.log import logger
from iotdb.mlnode.service import MLNode

server: MLNode = None


def main():
    global server
    arguments = sys.argv
    if len(arguments) == 1:
        logger.info("Command line argument must be specified.")
        return
    argument = sys.argv[1]
    if argument == 'start':
        server = MLNode()
        server.start()
    elif argument == 'stop':
        port = descriptor.get_config().get_mn_rpc_port()
        if platform.system() == "Windows":
            cmd = f"netstat -ano | findstr {port}"
            result = os.popen(cmd).read().strip().split()
            if result:
                pid = result[4]
                os.system(f"taskkill /F /PID {pid}")
                logger.info(f"Terminated process with PID {pid} that was using port {port}")
            else:
                logger.info(f"No process found using port {port}")
        else:
            cmd = f"lsof -i:{port} -t"
            pid = os.popen(cmd).read().strip()
            if pid:
                os.system(f"kill -9 {pid}")
                logger.info(f"Terminated process with PID {pid} that was using port {port}")
            else:
                logger.info(f"No process found using port {port}")
    else:
        logger.warning("Unknown argument: {}.".format(argument))
