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


class TemplateNode(object):
    """
    Template class, this class should be used to schema template node
    """

    def __init__(self, name):
        self.name = name

    def get_name(self):
        return self.name

    def get_children(self):
        return None

    def add_child(self, node):
        ...

    def delete_child(self, node):
        ...

    def is_measurement(self):
        return False

    def is_share_time(self):
        return False

    def serialize(self, *args, **kwargs):
        ...
