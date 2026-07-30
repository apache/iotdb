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

import gzip
import logging
import os
import random
import re
import shutil
import sys
import threading
from logging.handlers import TimedRotatingFileHandler

from iotdb.ainode.core.constant import (
    AINODE_LOG_DIR,
    AINODE_LOG_FILE_LEVELS,
    AINODE_LOG_FILE_NAME_PREFIX,
    DEFAULT_LOG_LEVEL,
    LOG_FILE_TYPE,
)
from iotdb.ainode.core.util.decorator import singleton


class BaseLogger:
    """
    BaseLogger is a base class for logging, which implements daily compress, log files management, custom format, etc.

    Args:
        log_file_name_prefix: the prefix of the log file name, it is used to distinguish different processes.
        log_dir: log directory, default is AINODE_HOME/logs.

    Attributes:
        logger_format: log format
        logger: global logger with custom format and level
        console_handler: console handler for stdout
        _lock: process lock for logger. This is just a precaution, we currently do not have multiprocessing
    """

    def __init__(self, log_file_name_prefix: str, log_dir=AINODE_LOG_DIR):
        self.logger_format = logging.Formatter(
            fmt="%(asctime)s %(levelname)s [%(process)d:%(processName)s] "
            "%(filename)s:%(funcName)s:%(lineno)d - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )

        self._lock = threading.Lock()

        self.logger = logging.getLogger(str(random.random()))
        self.logger.handlers.clear()
        self.logger.setLevel(DEFAULT_LOG_LEVEL)
        self.console_handler = logging.StreamHandler(sys.stdout)
        self.console_handler.setLevel(DEFAULT_LOG_LEVEL)
        self.console_handler.setFormatter(self.logger_format)
        self.logger.addHandler(self.console_handler)

        # Set log file handler
        for i in range(len(LOG_FILE_TYPE)):
            file_name = log_file_name_prefix + LOG_FILE_TYPE[i] + ".log"
            # create log file if not exist
            os.makedirs(log_dir, exist_ok=True)
            file_path = os.path.join(log_dir, f"{file_name}")
            if not os.path.exists(file_path):
                with open(file_path, "w", encoding="utf-8") as f:
                    f.write("")
            # create handler
            file_level = AINODE_LOG_FILE_LEVELS[i]
            file_handler = TimedRotatingFileHandler(
                filename=file_path,
                when="MIDNIGHT",
                interval=1,
                encoding="utf-8",
            )
            if LOG_FILE_TYPE[i] != "all":
                file_handler.setLevel(logging.NOTSET)
                file_handler.addFilter(LogLevelFilter(file_level))

            # set renamer
            def universal_namer(
                default_name: str, internal_file_name: str = file_name
            ) -> str:
                # to avoid outer variable late binding
                base, ext = os.path.splitext(internal_file_name)
                base = base.replace("_", "-")
                base_log_dir = os.path.dirname(default_name)
                suffix = default_name.rsplit(".", 1)[-1]  # e.g. 2025-08-01_13-45
                digits = re.sub(r"[-_]", "", suffix)
                return os.path.join(base_log_dir, f"{base}-{digits}{ext}.gz")

            file_handler.namer = universal_namer

            # set gzip
            def gzip_rotator(src: str, dst: str):
                with open(src, "rb") as f_in, gzip.open(dst, "wb") as f_out:
                    shutil.copyfileobj(f_in, f_out)
                # delete the old .log file
                os.remove(src)

            file_handler.rotator = gzip_rotator
            # other settings
            file_handler.setLevel(file_level)
            file_handler.setFormatter(self.logger_format)
            self.logger.addHandler(file_handler)

        self.info(
            f"Logger init successfully, log file prefix name {log_file_name_prefix}."
        )

    # interfaces
    def debug(self, *msg):
        self._write(self.logger.debug, *msg)

    def info(self, *msg):
        self._write(self.logger.info, *msg)

    def warning(self, *msg):
        self._write(self.logger.warning, *msg)

    def error(self, *msg):
        self._write(self.logger.error, *msg)

    def _write(self, function, *msg):
        with self._lock:
            # The stack level of caller is 3, because:
            # caller -> BaseLogger.info -> BaseLogger._write
            function(" ".join(map(str, msg)), stacklevel=3)


@singleton
class Logger(BaseLogger):
    """
    Logger is a singleton, just use Logger() to get it in the specified process.

    Args:
        log_file_name_prefix: the prefix of the log file name, it is used to distinguish inference, training and the main process.
    """

    def __init__(self, log_file_name_prefix: str = AINODE_LOG_FILE_NAME_PREFIX):
        super().__init__(log_file_name_prefix=log_file_name_prefix)


class LogLevelFilter(logging.Filter):

    def __init__(self, level: int):
        super().__init__()
        self.level = level

    def filter(self, record: logging.LogRecord) -> bool:
        return record.levelno == self.level
