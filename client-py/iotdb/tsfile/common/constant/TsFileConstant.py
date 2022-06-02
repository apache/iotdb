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


class TsFileConstant:
    TSFILE_SUFFIX = ".tsfile"
    TSFILE_HOME = "TSFILE_HOME"
    TSFILE_CONF = "TSFILE_CONF"
    PATH_ROOT = "root"
    TMP_SUFFIX = "tmp"
    PATH_SEPARATOR = "."
    PATH_SEPARATOR_CHAR = "."
    PATH_SEPARATER_NO_REGEX = "\\."
    DOUBLE_QUOTE = '"'

    TIME_COLUMN_MASK = 0x80

    VALUE_COLUMN_MASK = 0x40

    def __ts_file_constant(self):
        ...
