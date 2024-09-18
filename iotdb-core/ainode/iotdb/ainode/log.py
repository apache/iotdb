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
import inspect
import logging
import multiprocessing
import os
import random
import sys
import threading

from iotdb.ainode.constant import STD_LEVEL, AINODE_LOG_FILE_NAMES, AINODE_LOG_FILE_LEVELS
from iotdb.ainode.util.decorator import singleton


class LoggerFilter(logging.Filter):
    def filter(self, record):
        record.msg = f"{self.custom_log_info()}: {record.msg}"
        return True

    @staticmethod
    def custom_log_info():
        frame = inspect.currentframe()
        stack_trace = inspect.getouterframes(frame)

        pid = os.getpid()
        process_name = multiprocessing.current_process().name

        stack_info = ""
        frame_info = stack_trace[7]
        file_name = frame_info.filename
        # if file_name is not in current working directory, find the first "iotdb" in the path
        for l in range(len(file_name)):
            i = len(file_name) - l - 1
            if file_name[i:].startswith("iotdb/") or file_name[i:].startswith("iotdb\\"):
                file_name = file_name[i:]
                break

        stack_info += f"{file_name}:{frame_info.lineno}-{frame_info.function}"

        return f"[{pid}:{process_name}] {stack_info}"


@singleton
class Logger:
    """ Logger is a singleton, it will be initialized when AINodeDescriptor is inited for the first time.
        You can just use Logger() to get it anywhere.

    Args:
        log_dir: log directory

    logger_format: log format
    logger: global logger with custom format and level
    file_handlers: file handlers for different levels
    console_handler: console handler for stdout
    _lock: process lock for logger. This is just a precaution, we currently do not have multiprocessing
    """

    def __init__(self, log_dir=None):

        self.logger_format = logging.Formatter(fmt='%(asctime)s %(levelname)s %('
                                                   'message)s',
                                               datefmt='%Y-%m-%d %H:%M:%S')

        self.logger = logging.getLogger(str(random.random()))
        self.logger.handlers.clear()
        self.logger.setLevel(logging.DEBUG)
        self.console_handler = logging.StreamHandler(sys.stdout)
        self.console_handler.setLevel(STD_LEVEL)
        self.console_handler.setFormatter(self.logger_format)

        self.logger.addHandler(self.console_handler)

        if log_dir is not None:
            file_names = AINODE_LOG_FILE_NAMES
            file_levels = AINODE_LOG_FILE_LEVELS
            if not os.path.exists(log_dir):
                os.makedirs(log_dir)
                os.chmod(log_dir, 0o777)
            for file_name in file_names:
                log_path = log_dir + "/" + file_name
                if not os.path.exists(log_path):
                    f = open(log_path, mode='w', encoding='utf-8')
                    f.close()
                    os.chmod(log_path, 0o777)
            self.file_handlers = []
            for l in range(len(file_names)):
                self.file_handlers.append(logging.FileHandler(log_dir + "/" + file_names[l], mode='a'))
                self.file_handlers[l].setLevel(file_levels[l])
                self.file_handlers[l].setFormatter(self.logger_format)

            for file_handler in self.file_handlers:
                self.logger.addHandler(file_handler)
        else:
            log_dir = "default path"

        self.logger.addFilter(LoggerFilter())
        self._lock = threading.Lock()
        self.info(f"Logger init successfully. Log will be written to {log_dir}")

    def debug(self, *args) -> None:
        self._lock.acquire()
        self.logger.debug(' '.join(map(str, args)))
        self._lock.release()

    def info(self, *args) -> None:
        self._lock.acquire()
        self.logger.info(' '.join(map(str, args)))
        self._lock.release()

    def warning(self, *args) -> None:
        self._lock.acquire()
        self.logger.warning(' '.join(map(str, args)))
        self._lock.release()

    def error(self, *args) -> None:
        self._lock.acquire()
        self.logger.error(' '.join(map(str, args)))
        self._lock.release()
