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

import struct
import warnings

from .TemplateNode import TemplateNode
from ..tsfile.common.constant.TsFileConstant import TsFileConstant
from ..tsfile.utils.Pair import Pair
from ..tsfile.utils.ReadWriteIOUtils import ReadWriteUtils

warnings.simplefilter("always", DeprecationWarning)


class Template:
    def __init__(self, name, share_time: bool = False):
        warnings.warn(
            "The APIs about template are deprecated and will be removed in future versions. Use sql instead.",
            DeprecationWarning,
            stacklevel=2,
        )
        self.name = name
        self.children = dict()
        self.share_time = share_time

    def get_name(self) -> object:
        return self.name

    def is_share_time(self) -> object:
        return self.share_time

    def set_share_time(self, share_time: bool):
        self.share_time = share_time

    def add_template(self, child: TemplateNode):
        if self.children.get(child.get_name()):
            raise Exception("Duplicated child of node in template.")
        self.children.update({child.get_name(): child})

    def delete_from_template(self, name: str):
        if not self.children.pop(name, []):
            raise Exception("It is not a direct child of the template: " + name)

    @property
    def serialize(self):
        format_str_list = [">"]
        values_tobe_packed = []
        stack = []
        aligned_prefix = set()
        ReadWriteUtils.write(self.get_name(), format_str_list, values_tobe_packed)
        ReadWriteUtils.write(self.is_share_time(), format_str_list, values_tobe_packed)
        if self.is_share_time():
            aligned_prefix.add("")

        for child in self.children:
            stack.append(Pair("", self.children[child]))

        while stack:
            pair = stack.pop()
            prefix = pair.left
            cur_node = pair.right
            full_path = [prefix]
            if not cur_node.is_measurement():
                if prefix != "":
                    full_path.append(TsFileConstant.PATH_SEPARATOR)
                full_path.append(cur_node.get_name())
                if cur_node.is_share_time():
                    aligned_prefix.add("".join(full_path))
                for child in cur_node.children:
                    stack.append(Pair("".join(full_path), cur_node.children[child]))
            else:
                ReadWriteUtils.write(prefix, format_str_list, values_tobe_packed)
                if prefix in aligned_prefix:
                    ReadWriteUtils.write(True, format_str_list, values_tobe_packed)
                else:
                    ReadWriteUtils.write(False, format_str_list, values_tobe_packed)
                cur_node.serialize(format_str_list, values_tobe_packed)

        format_str = "".join(format_str_list)
        return struct.pack(format_str, *values_tobe_packed)
