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



class _BaseError(Exception):
    """Base class for exceptions in this module."""
    pass


class BadNodeUrlError(_BaseError):
    def __init__(self, node_url: str):
        self.message = "Bad node url: {}".format(node_url)


class ModelNotExistError(_BaseError):
    def __init__(self, file_path: str):
        self.message = "Model path: ({}) not exists".format(file_path)


class BadConfigError(_BaseError):
    def __init__(self, msg):
        self.message = "Bad config: {}".format(msg)
