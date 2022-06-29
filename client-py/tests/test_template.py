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
        assert session.count_measurements_in_template(measurement_template_name) == 3
        assert (
            session.is_measurement_in_template(measurement_template_name, "s1") is True
        )
        assert (
            session.is_path_exist_in_template(measurement_template_name, "s1") is True
        )
        assert (
            session.is_path_exist_in_template(measurement_template_name, "s4") is False
        )

        session.delete_node_in_template(measurement_template_name, "s1")
        assert session.show_measurements_in_template(measurement_template_name) == [
            "s3",
            "s2",
        ]
        assert session.count_measurements_in_template(measurement_template_name) == 2
        assert (
            session.is_path_exist_in_template(measurement_template_name, "s1") is False
        )

        tree_template_name = "treeTemplate_python"
        template = Template(name=tree_template_name, share_time=True)
        i_node_gps = InternalNode(name="GPS", share_time=False)
        i_node_v = InternalNode(name="vehicle", share_time=True)
        m_node_x = MeasurementNode(
            "x", TSDataType.FLOAT, TSEncoding.RLE, Compressor.SNAPPY
        )

        i_node_gps.add_child(m_node_x)
        i_node_v.add_child(m_node_x)
        template.add_template(i_node_gps)
        template.add_template(i_node_v)
        template.add_template(m_node_x)
        session.create_schema_template(template)
        assert session.show_measurements_in_template(tree_template_name) == [
            "x",
            "GPS.x",
            "vehicle.x",
        ]
        assert session.count_measurements_in_template(tree_template_name) == 3

        assert session.show_all_templates() == [
            measurement_template_name,
            tree_template_name,
        ]
        assert session.is_measurement_in_template(tree_template_name, "GPS") is False
        assert session.is_measurement_in_template(tree_template_name, "GPS.x") is True

        session.drop_schema_template(measurement_template_name)
        session.drop_schema_template(tree_template_name)

        session.close()


def test_add_measurements_template():
    with IoTDBContainer("iotdb:dev") as db:
        db: IoTDBContainer
        session = Session(db.get_container_host_ip(), db.get_exposed_port(6667))
        session.open(False)

        template_name = "add_template_python"
        template = Template(name=template_name, share_time=False)
        i_node_v = InternalNode(name="GPS", share_time=False)
        i_node_gps_x = MeasurementNode(
            "x", TSDataType.FLOAT, TSEncoding.RLE, Compressor.SNAPPY
        )

        i_node_v.add_child(i_node_gps_x)
        template.add_template(i_node_v)
        session.create_schema_template(template)

        # # append schema template
        data_types = [TSDataType.FLOAT, TSDataType.FLOAT, TSDataType.DOUBLE]
        encoding_list = [TSEncoding.RLE, TSEncoding.RLE, TSEncoding.GORILLA]
        compressor_list = [Compressor.SNAPPY, Compressor.SNAPPY, Compressor.LZ4]

        measurements_aligned_path = ["aligned.s1", "aligned.s2", "aligned.s3"]
        session.add_measurements_in_template(
            template_name,
            measurements_aligned_path,
            data_types,
            encoding_list,
            compressor_list,
            is_aligned=True,
        )
        # session.drop_schema_template("add_template_python")
        measurements_aligned_path = ["unaligned.s1", "unaligned.s2", "unaligned.s3"]
        session.add_measurements_in_template(
            template_name,
            measurements_aligned_path,
            data_types,
            encoding_list,
            compressor_list,
            is_aligned=False,
        )
        measurements_aligned_path = ["s1", "s2", "s3"]
        session.add_measurements_in_template(
            template_name,
            measurements_aligned_path,
            data_types,
            encoding_list,
            compressor_list,
            is_aligned=False,
        )

        assert session.count_measurements_in_template(template_name) == 10
        assert session.is_measurement_in_template(template_name, "GPS") is False
        assert session.is_path_exist_in_template(template_name, "GPS.x") is True
        assert session.is_path_exist_in_template(template_name, "x") is False

        session.drop_schema_template(template_name)
        session.close()


def test_set_template():
    with IoTDBContainer("iotdb:dev") as db:
        db: IoTDBContainer
        session = Session(db.get_container_host_ip(), db.get_exposed_port(6667))
        session.open(False)

        template_name = "set_template_python"
        template = Template(name=template_name, share_time=False)
        session.create_schema_template(template)

        session.set_schema_template(template_name, "root.python.GPS")
        session.execute_non_query_statement("create timeseries of schema template on root.python.GPS")

        assert session.show_paths_template_set_on(template_name) == ["root.python.GPS"]
        assert session.show_paths_template_using_on(template_name) == ["root.python.GPS"]

        session.unset_schema_template(template_name, "root.python.GPS")
        session.drop_schema_template(template_name)
        session.close()
