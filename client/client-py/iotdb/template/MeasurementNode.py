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

from iotdb.utils.IoTDBConstants import TSDataType, TSEncoding, Compressor
from .TemplateNode import TemplateNode
from ..tsfile.utils.ReadWriteIOUtils import ReadWriteUtils


class MeasurementNode(TemplateNode):
    def __init__(
        self,
        name: str,
        data_type: TSDataType,
        encoding: TSEncoding,
        compression_type: Compressor,
    ):
        self.name = name
        self.data_type = data_type
        self.encoding = encoding
        self.compression_type = compression_type

    def is_measurement(self):
        return True

    def get_data_type(self):
        return self.data_type

    def get_encoding(self):
        return self.encoding

    def get_compression_type(self):
        return self.compression_type

    def serialize(self, *args, **kwargs):
        format_str_list, values_tobe_packed = args
        ReadWriteUtils.write(self.get_name(), format_str_list, values_tobe_packed)
        ReadWriteUtils.write(self.get_data_type(), format_str_list, values_tobe_packed)
        ReadWriteUtils.write(self.get_encoding(), format_str_list, values_tobe_packed)
        ReadWriteUtils.write(
            self.get_compression_type(), format_str_list, values_tobe_packed
        )
