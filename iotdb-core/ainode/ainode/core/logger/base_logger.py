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
import os
import random
import sys
import threading
from logging.handlers import TimedRotatingFileHandler

from ainode.core.constant import (
    AINODE_LOG_DIR,
    AINODE_LOG_FILE_LEVELS,
    AINODE_LOG_FILE_NAMES,
    STD_LEVEL,
)


class BaseLogger:

    def __init__(
            self,
            sub_dir: str
    ):
        self.logger_format = logging.Formatter(
            fmt="%(asctime)s %(levelname)s [%(process)d:%(processName)s] " \
                "%(filename)s:%(funcName)s:%(lineno)d - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S"
        )

        self._lock = threading.Lock()

        self.logger = logging.getLogger(str(random.random()))
        self.logger.handlers.clear()
        self.logger.setLevel(logging.DEBUG)
        self.console_handler = logging.StreamHandler(sys.stdout)
        self.console_handler.setLevel(STD_LEVEL)
        self.console_handler.setFormatter(self.logger_format)
        self.logger.addHandler(self.console_handler)

        target_dir = os.path.join(AINODE_LOG_DIR, sub_dir)
        os.makedirs(target_dir, exist_ok=True)
        os.chmod(target_dir, 0o755)

        for i in range(len(AINODE_LOG_FILE_NAMES)):
            file_name = AINODE_LOG_FILE_NAMES[i]
            # create log file if not exist
            file_path = os.path.join(target_dir, f"{file_name}")
            if not os.path.exists(file_path):
                with open(file_path, "w", encoding="utf-8"):
                    pass
                os.chmod(file_path, 0o644)
            # create handler
            file_level = AINODE_LOG_FILE_LEVELS[i]
            fh = TimedRotatingFileHandler(
                filename=os.path.join(target_dir, f"{file_name}"),
                when="MIDNIGHT",
                interval=1,
                encoding="utf-8",
            )
            fh.setLevel(file_level)
            fh.setFormatter(self.logger_format)
            self.logger.addHandler(fh)

        self.info(f"Logger init successfully. Log will be written to {target_dir}")

    # interfaces
    def debug(self, *msg):   self._write(self.logger.debug,   *msg)
    def info(self, *msg):    self._write(self.logger.info,    *msg)
    def warning(self, *msg): self._write(self.logger.warning, *msg)
    def error(self, *msg):   self._write(self.logger.error,   *msg)

    def _write(self, function, *msg):
        with self._lock:
            function(" ".join(map(str, msg)), stacklevel=3)