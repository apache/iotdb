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
from iotdb.IoTDBContainer import IoTDBContainer
from iotdb.Session import Session
from iotdb.template.InternalNode import InternalNode
from iotdb.template.MeasurementNode import MeasurementNode
from iotdb.template.Template import Template
from iotdb.utils.IoTDBConstants import TSDataType, Compressor, TSEncoding


def test_template_create():
    with IoTDBContainer("iotdb:dev") as db:
        db: IoTDBContainer
        session = Session(db.get_container_host_ip(), db.get_exposed_port(6667))
        session.open(False)

        template = Template(name="template_python", share_time=False)
        m_node_1 = MeasurementNode(
            name="s1",
            data_type=TSDataType.INT64,
            encoding=TSEncoding.RLE,
            compression_type=Compressor.SNAPPY,
        )
        m_node_2 = MeasurementNode(
            name="s2",
            data_type=TSDataType.INT64,
            encoding=TSEncoding.RLE,
            compression_type=Compressor.SNAPPY,
        )
        m_node_3 = MeasurementNode(
            name="s3",
            data_type=TSDataType.INT64,
            encoding=TSEncoding.RLE,
            compression_type=Compressor.SNAPPY,
        )
        template.add_template(m_node_1)
        template.add_template(m_node_2)
        template.add_template(m_node_3)
        session.create_schema_template(template)

        template = Template(name="treeTemplate_python", share_time=True)
        i_node_gps = InternalNode(name="GPS", share_time=False)
        i_node_v = InternalNode(name="vehicle", share_time=True)
        m_node_x = MeasurementNode(
            "x", TSDataType.FLOAT, TSEncoding.RLE, Compressor.SNAPPY
        )

        i_node_gps.add_child(m_node_x)
        i_node_gps.add_child(m_node_x)
        i_node_v.add_child(m_node_x)
        template.add_template(i_node_gps)
        template.add_template(i_node_v)
        template.add_template(m_node_x)
        session.create_schema_template(template)

        session.drop_schema_template("template_python")
        session.drop_schema_template("treeTemplate_python")

        session.close()
