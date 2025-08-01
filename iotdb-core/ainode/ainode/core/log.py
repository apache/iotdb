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
from ainode.core.constant import AINODE_LOG_FILE_NAME_PREFIX
from ainode.core.logger.base_logger import BaseLogger
from ainode.core.util.decorator import singleton


@singleton
class Logger(BaseLogger):
    """Logger is a singleton, it will be initialized when AINodeDescriptor is inited for the first time.
        You can just use Logger() to get it anywhere.

    Args:
        log_dir: log directory

    logger_format: log format
    logger: global logger with custom format and level
    file_handlers: file handlers for different levels
    console_handler: console handler for stdout
    _lock: process lock for logger. This is just a precaution, we currently do not have multiprocessing
    """

    def __init__(self, log_file_name_prefix: str = AINODE_LOG_FILE_NAME_PREFIX):
        super().__init__(log_file_name_prefix=log_file_name_prefix)
