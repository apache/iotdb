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


from enum import Enum, unique


@unique
class TemplateQueryType(Enum):
    COUNT_MEASUREMENTS = 0
    IS_MEASUREMENT = 1
    PATH_EXIST = 2
    SHOW_MEASUREMENTS = 3
    SHOW_TEMPLATES = 4
    SHOW_SET_TEMPLATES = 5
    SHOW_USING_TEMPLATES = 6

    # this method is implemented to avoid the issue reported by:
    # https://bugs.python.org/issue30545
    def __eq__(self, other) -> bool:
        return self.value == other.value

    def __hash__(self):
        return self.value
