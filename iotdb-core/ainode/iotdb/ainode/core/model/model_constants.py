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

# Model file constants
MODEL_WEIGHTS_FILE_IN_SAFETENSORS = "model.safetensors"
MODEL_CONFIG_FILE_IN_JSON = "config.json"
MODEL_WEIGHTS_FILE_IN_PT = "model.pt"
MODEL_CONFIG_FILE_IN_YAML = "config.yaml"


class ModelCategory(Enum):
    BUILTIN = "builtin"
    USER_DEFINED = "user_defined"


class ModelStates(Enum):
    INACTIVE = "inactive"
    ACTIVATING = "activating"
    ACTIVE = "active"
    DROPPING = "dropping"


class UriType(Enum):
    REPO = "repo"
    FILE = "file"
