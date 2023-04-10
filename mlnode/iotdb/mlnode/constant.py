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
from enum import Enum

MLNODE_CONF_DIRECTORY_NAME = "conf"
MLNODE_CONF_FILE_NAME = "iotdb-mlnode.toml"
MLNODE_LOG_CONF_FILE_NAME = "logging_config.ini"

MLNODE_MODEL_STORAGE_DIRECTORY_NAME = "models"


class TSStatusCode(Enum):
    SUCCESS_STATUS = 200
    REDIRECTION_RECOMMEND = 400
    MLNODE_INTERNAL_ERROR = 1510

    def get_status_code(self) -> int:
        return self.value
