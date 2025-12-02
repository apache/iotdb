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
import multiprocessing
import sys

import torch.multiprocessing as mp

from iotdb.ainode.core.ai_node import AINode
from iotdb.ainode.core.log import Logger

logger = Logger()


def main():
    # Handle PyInstaller: filter out Python arguments that might be passed to subprocesses
    # These arguments are not needed in frozen executables and cause warnings
    # Note: This filtering should happen AFTER freeze_support() has handled child processes
    if getattr(sys, "frozen", False):
        python_args_to_filter = ["-I", "-B", "-S", "-E", "-O", "-OO"]
        sys.argv = [arg for arg in sys.argv if arg not in python_args_to_filter]

    logger.info(f"Starting IoTDB-AINode process with sys argv {sys.argv}.")
    arguments = sys.argv
    if len(arguments) == 1:
        logger.info("Command line argument must be specified.")
        return
    command = arguments[1]
    if command == "start":
        try:
            mp.set_start_method("spawn", force=True)
            logger.info(f"Current multiprocess start method: {mp.get_start_method()}")
            logger.info("IoTDB-AINode is starting...")
            ai_node = AINode()
            ai_node.start()
        except Exception as e:
            logger.warning("Start AINode failed, because of: {}".format(e))
            sys.exit(1)
    else:
        logger.warning("Unknown argument: {}.".format(command))


if __name__ == "__main__":
    # PyInstaller multiprocessing support
    # freeze_support() is essential for PyInstaller frozen executables on all platforms
    # It detects if the current process is a multiprocessing child process
    # If it is, it executes the child process target function and exits
    # If it's not, it returns immediately and continues with main() execution
    # This prevents child processes from executing the main application logic
    if getattr(sys, "frozen", False):
        # Call freeze_support() for both standard multiprocessing and torch.multiprocessing
        multiprocessing.freeze_support()
        mp.freeze_support()

    main()
