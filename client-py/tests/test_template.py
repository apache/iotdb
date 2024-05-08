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

        measurement_template_name = "template_python"
        template = Template(name=measurement_template_name, share_time=False)
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

        assert session.show_measurements_in_template(measurement_template_name) == [
            "s3",
            "s1",
            "s2",
        ]

        session.drop_schema_template(measurement_template_name)

        session.close()


def test_set_template():
    with IoTDBContainer("iotdb:dev") as db:
        db: IoTDBContainer
        session = Session(db.get_container_host_ip(), db.get_exposed_port(6667))
        session.open(False)

        template_name = "set_template_python"
        template = Template(name=template_name, share_time=False)
        m_node_x = MeasurementNode(
            name="x",
            data_type=TSDataType.INT64,
            encoding=TSEncoding.RLE,
            compression_type=Compressor.SNAPPY,
        )
        m_node_y = MeasurementNode(
            name="y",
            data_type=TSDataType.INT64,
            encoding=TSEncoding.RLE,
            compression_type=Compressor.SNAPPY,
        )
        template.add_template(m_node_x)
        template.add_template(m_node_y)
        session.create_schema_template(template)

        session.execute_non_query_statement("CREATE DATABASE root.python")

        session.set_schema_template(template_name, "root.python.GPS")
        session.execute_non_query_statement("create timeseries of schema template on root.python.GPS")

        assert session.show_paths_template_set_on(template_name) == ["root.python.GPS"]
        assert session.show_paths_template_using_on(template_name) == ["root.python.GPS"]

        session.execute_non_query_statement("delete timeseries of schema template from root.python.GPS")

        session.unset_schema_template(template_name, "root.python.GPS")
        session.drop_schema_template(template_name)
        session.close()
