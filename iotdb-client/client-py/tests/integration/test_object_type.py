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
import numpy as np

from iotdb.Session import Session
from iotdb.utils.IoTDBConstants import TSDataType, TSEncoding, Compressor
from iotdb.utils.NumpyTablet import NumpyTablet
from iotdb.utils.Tablet import Tablet
from .iotdb_container import IoTDBContainer


def test_object_type_tablet_read_write():
    with IoTDBContainer() as db:
        session = Session(db.get_container_host_ip(), db.get_exposed_port(6667))
        session.open(False)
        assert session.is_open()

        storage = "root.sg_object_py"
        device = f"{storage}.d1"
        measurement = "obj_m1"
        path = f"{device}.{measurement}"

        session.set_storage_group(storage)
        session.create_time_series(
            path,
            TSDataType.OBJECT,
            TSEncoding.PLAIN,
            Compressor.SNAPPY,
        )

        payloads = [b"\x01\x02\x03", b"payload-b", b""]
        timestamps = [100, 200, 300]
        values = [[p] for p in payloads]

        tablet = Tablet(
            device,
            [measurement],
            [TSDataType.OBJECT],
            values,
            timestamps,
        )
        session.insert_tablet(tablet)

        np_tablet = NumpyTablet(
            device,
            [measurement],
            [TSDataType.OBJECT],
            [np.array([b"\xaa\xbb", b"\xcc"], dtype=object)],
            np.array([400, 500], dtype=TSDataType.INT64.np_dtype()),
        )
        session.insert_tablet(np_tablet)

        session.insert_records(
            [device, device],
            [600, 700],
            [[measurement], [measurement]],
            [[TSDataType.OBJECT], [TSDataType.OBJECT]],
            [[b"\xde\xad"], [b"\xbe\xef"]],
        )

        with session.execute_query_statement(
            f"select {measurement} from {device}"
        ) as dataset:
            rows = []
            while dataset.has_next():
                rows.append(dataset.next())
            assert len(rows) == 7
            for i, row in enumerate(rows):
                f = row.get_fields()[0]
                assert f.get_data_type() == TSDataType.OBJECT
                assert (
                    f.get_binary_value()
                    == [
                        payloads[0],
                        payloads[1],
                        payloads[2],
                        b"\xaa\xbb",
                        b"\xcc",
                        b"\xde\xad",
                        b"\xbe\xef",
                    ][i]
                )
                assert f.get_object_value(TSDataType.OBJECT) == f.get_binary_value()

        session.close()
