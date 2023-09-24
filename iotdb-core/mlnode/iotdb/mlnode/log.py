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
import multiprocessing
import inspect

# log directory
LOG_DIR = "./logs"
# level fot stdout
STD_LEVEL = logging.INFO


class LoggerFilter(logging.Filter):
    def filter(self, record):
        record.msg = f"{self.custom_log_info()} Message: {record.msg}"
        return True

    @staticmethod
    def custom_log_info():
        frame = inspect.currentframe()
        stack_trace = inspect.getouterframes(frame)

        pid = os.getpid()
        process_name = multiprocessing.current_process().name

        stack_info = ""
        frame_info = stack_trace[7]
        stack_info += f"File: {frame_info.filename}, Line: {frame_info.lineno}, Function: {frame_info.function}"

        return f"PID: {pid} PName: {process_name} Stack Trace:{stack_info}"


class Logger:
    """
    Args:
        log_dir: log directory

    logger_format: log format of global logger
    logger: global logger with custom format and level
    file_handlers: file handlers for different levels
    console_handler: console handler for stdout
    __lock: lock for logger
    """
    def __init__(self, log_dir=LOG_DIR):
        file_names = ['log_mlnode_debug.log', 'log_mlnode_info.log', 'log_mlnode_warning.log', 'log_mlnode_error.log']
        file_levels = [logging.DEBUG, logging.INFO, logging.WARNING, logging.ERROR]

        if not os.path.exists(log_dir):
            os.mkdir(log_dir)
            os.chmod(log_dir, 0o777)
        for file_name in file_names:
            log_path = log_dir + "/" + file_name
            if not os.path.exists(log_path):
                f = open(log_path, mode='w', encoding='utf-8')
                f.close()
                os.chmod(log_path, 0o777)
        self.logger_format = logging.Formatter(fmt='%(asctime)s Level: %(levelname)s %('
                                                   'message)s',
                                               datefmt='%Y-%m-%d %H:%M:%S')

        self.logger = logging.getLogger(str(random.random()))
        self.logger.handlers.clear()
        self.logger.setLevel(logging.DEBUG)

        self.file_handlers = []
        for l in range(len(file_names)):
            self.file_handlers.append(logging.FileHandler(log_dir + "/" + file_names[l], mode='a'))
            self.file_handlers[l].setLevel(file_levels[l])
            self.file_handlers[l].setFormatter(self.logger_format)

        self.console_handler = logging.StreamHandler(sys.stdout)
        self.console_handler.setLevel(STD_LEVEL)
        self.console_handler.setFormatter(self.logger_format)

        self.logger.addHandler(self.console_handler)
        for filehandler in self.file_handlers:
            self.logger.addHandler(filehandler)

        self.logger.addFilter(LoggerFilter())
        self.__lock = multiprocessing.Lock()

    def debug(self, *args) -> None:
        self.__lock.acquire()
        self.logger.debug(' '.join(map(str, args)))
        self.__lock.release()

    def info(self, *args) -> None:
        self.__lock.acquire()
        self.logger.info(' '.join(map(str, args)))
        self.__lock.release()

    def warning(self, *args) -> None:
        self.__lock.acquire()
        self.logger.warning(' '.join(map(str, args)))
        self.__lock.release()

    def error(self, *args) -> None:
        self.__lock.acquire()
        self.logger.error(' '.join(map(str, args)))
        self.__lock.release()


logger = Logger()
