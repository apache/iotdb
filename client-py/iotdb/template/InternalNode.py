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

from .TemplateNode import TemplateNode


class InternalNode(TemplateNode):
    def __init__(self, name, share_time):
        super().__init__(name)
        self.children = {}
        self.share_time = share_time

    def add_child(self, node: TemplateNode):
        if node.get_name() in self.children.keys():
            assert "Duplicated child of node in template."

        self.children.update({node.get_name(): node})

    def delete_child(self, node):
        self.children.pop(node.get_name(), None)

    def get_children(self):
        return self.children

    def is_share_time(self):
        return self.share_time
