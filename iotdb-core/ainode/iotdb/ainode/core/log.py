1# Licensed to the Apache Software Foundation (ASF) under one
1# or more contributor license agreements.  See the NOTICE file
1# distributed with this work for additional information
1# regarding copyright ownership.  The ASF licenses this file
1# to you under the Apache License, Version 2.0 (the
1# "License"); you may not use this file except in compliance
1# with the License.  You may obtain a copy of the License at
1#
1#     http://www.apache.org/licenses/LICENSE-2.0
1#
1# Unless required by applicable law or agreed to in writing,
1# software distributed under the License is distributed on an
1# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
1# KIND, either express or implied.  See the License for the
1# specific language governing permissions and limitations
1# under the License.
1#
1
1import gzip
1import logging
1import os
1import random
1import re
1import shutil
1import sys
1import threading
1from logging.handlers import TimedRotatingFileHandler
1
1from iotdb.ainode.core.constant import (
1    AINODE_LOG_DIR,
1    AINODE_LOG_FILE_LEVELS,
1    AINODE_LOG_FILE_NAME_PREFIX,
1    DEFAULT_LOG_LEVEL,
1    LOG_FILE_TYPE,
1)
1from iotdb.ainode.core.util.decorator import singleton
1
1
1class BaseLogger:
1    """
1    BaseLogger is a base class for logging, which implements daily compress, log files management, custom format, etc.
1
1    Args:
1        log_file_name_prefix: the prefix of the log file name, it is used to distinguish different processes.
1        log_dir: log directory, default is AINODE_HOME/logs.
1
1    Attributes:
1        logger_format: log format
1        logger: global logger with custom format and level
1        console_handler: console handler for stdout
1        _lock: process lock for logger. This is just a precaution, we currently do not have multiprocessing
1    """
1
1    def __init__(self, log_file_name_prefix: str, log_dir=AINODE_LOG_DIR):
1        self.logger_format = logging.Formatter(
1            fmt="%(asctime)s %(levelname)s [%(process)d:%(processName)s] "
1            "%(filename)s:%(funcName)s:%(lineno)d - %(message)s",
1            datefmt="%Y-%m-%d %H:%M:%S",
1        )
1
1        self._lock = threading.Lock()
1
1        self.logger = logging.getLogger(str(random.random()))
1        self.logger.handlers.clear()
1        self.logger.setLevel(DEFAULT_LOG_LEVEL)
1        self.console_handler = logging.StreamHandler(sys.stdout)
1        self.console_handler.setLevel(DEFAULT_LOG_LEVEL)
1        self.console_handler.setFormatter(self.logger_format)
1        self.logger.addHandler(self.console_handler)
1
1        # Set log file handler
1        for i in range(len(LOG_FILE_TYPE)):
1            file_name = log_file_name_prefix + LOG_FILE_TYPE[i] + ".log"
1            # create log file if not exist
1            os.makedirs(log_dir, exist_ok=True)
1            file_path = os.path.join(log_dir, f"{file_name}")
1            if not os.path.exists(file_path):
1                with open(file_path, "w", encoding="utf-8") as f:
1                    f.write("")
1            # create handler
1            file_level = AINODE_LOG_FILE_LEVELS[i]
1            file_handler = TimedRotatingFileHandler(
1                filename=file_path,
1                when="MIDNIGHT",
1                interval=1,
1                encoding="utf-8",
1            )
1            if LOG_FILE_TYPE[i] != "all":
1                file_handler.setLevel(logging.NOTSET)
1                file_handler.addFilter(LogLevelFilter(file_level))
1
1            # set renamer
1            def universal_namer(
1                default_name: str, internal_file_name: str = file_name
1            ) -> str:
1                # to avoid outer variable late binding
1                base, ext = os.path.splitext(internal_file_name)
1                base = base.replace("_", "-")
1                base_log_dir = os.path.dirname(default_name)
1                suffix = default_name.rsplit(".", 1)[-1]  # e.g. 2025-08-01_13-45
1                digits = re.sub(r"[-_]", "", suffix)
1                return os.path.join(base_log_dir, f"{base}-{digits}{ext}.gz")
1
1            file_handler.namer = universal_namer
1
1            # set gzip
1            def gzip_rotator(src: str, dst: str):
1                with open(src, "rb") as f_in, gzip.open(dst, "wb") as f_out:
1                    shutil.copyfileobj(f_in, f_out)
1                # delete the old .log file
1                os.remove(src)
1
1            file_handler.rotator = gzip_rotator
1            # other settings
1            file_handler.setLevel(file_level)
1            file_handler.setFormatter(self.logger_format)
1            self.logger.addHandler(file_handler)
1
1        self.info(f"Logger init successfully.")
1
1    # interfaces
1    def debug(self, *msg):
1        self._write(self.logger.debug, *msg)
1
1    def info(self, *msg):
1        self._write(self.logger.info, *msg)
1
1    def warning(self, *msg):
1        self._write(self.logger.warning, *msg)
1
1    def error(self, *msg):
1        self._write(self.logger.error, *msg)
1
1    def _write(self, function, *msg):
1        with self._lock:
1            # The stack level of caller is 3, because:
1            # caller -> BaseLogger.info -> BaseLogger._write
1            function(" ".join(map(str, msg)), stacklevel=3)
1
1
1@singleton
1class Logger(BaseLogger):
1    """
1    Logger is a singleton, just use Logger() to get it in the specified process.
1
1    Args:
1        log_file_name_prefix: the prefix of the log file name, it is used to distinguish inference, training and the main process.
1    """
1
1    def __init__(self, log_file_name_prefix: str = AINODE_LOG_FILE_NAME_PREFIX):
1        super().__init__(log_file_name_prefix=log_file_name_prefix)
1
1
1class LogLevelFilter(logging.Filter):
1
1    def __init__(self, level: int):
1        super().__init__()
1        self.level = level
1
1    def filter(self, record: logging.LogRecord) -> bool:
1        return record.levelno == self.level
1