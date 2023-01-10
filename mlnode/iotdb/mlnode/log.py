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
from logging.config import fileConfig

from iotdb.mlnode.constant import (MLNODE_CONF_DIRECTORY_NAME,
                                   MLNODE_LOG_CONF_FILE_NAME)

log_conf_file = os.path.join(os.getcwd(), MLNODE_CONF_DIRECTORY_NAME, MLNODE_LOG_CONF_FILE_NAME)
if os.path.exists(log_conf_file):
    fileConfig(log_conf_file)
else:
    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

logger = logging.getLogger()
