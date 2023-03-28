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

import argparse
import random
import time
import numpy as np
import pandas as pd

from iotdb.Session import Session
from iotdb.utils.IoTDBConstants import TSDataType
from iotdb.utils.NumpyTablet import NumpyTablet
from iotdb.utils.Tablet import Tablet

# the data type specified the byte order (i.e. endian)
FORMAT_CHAR_OF_TYPES = {
    TSDataType.BOOLEAN: ">?",
    TSDataType.FLOAT: ">f4",
    TSDataType.DOUBLE: ">f8",
    TSDataType.INT32: ">i4",
    TSDataType.INT64: ">i8",
    TSDataType.TEXT: "str",
}

# the time column name in the csv file.
TIME_STR = "time"


def load_csv_data(measure_tstype_infos: dict, data_file_name: str) -> pd.DataFrame:
    """
    load csv data.
    :param measure_tstype_infos: key(str): measurement name, value(TSDataType): measurement data type
    :param data_file_name: the csv file name to load
    :return: data in format of pd.DataFrame.
    """
    metadata_for_pd = {TIME_STR: FORMAT_CHAR_OF_TYPES[TSDataType.INT64]}
    for _measure, _type in measure_tstype_infos.items():
        metadata_for_pd[_measure] = FORMAT_CHAR_OF_TYPES[_type]
    df = pd.read_csv(data_file_name, dtype=metadata_for_pd)
    return df


def generate_csv_data(
    measure_tstype_infos: dict, data_file_name: str, _row: int, seed=0
) -> None:
    """
    generate csv data randomly according to given measurements and their data types.
    :param measure_tstype_infos: key(str): measurement name, value(TSDataType): measurement data type
    :param data_file_name: the csv file name to output
    :param _row: tablet row number
    :param seed: random seed
    """
    import random

    random.seed(seed)

    CHAR_BASE = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

    def generate_data(_type: TSDataType):
        if _type == TSDataType.BOOLEAN:
            return [random.randint(0, 1) == 1 for _ in range(_row)]
        elif _type == TSDataType.INT32:
            return [random.randint(-(2**31), 2**31) for _ in range(_row)]
        elif _type == TSDataType.INT64:
            return [random.randint(-(2**63), 2**63) for _ in range(_row)]
        elif _type == TSDataType.FLOAT:
            return [1.5 for _ in range(_row)]
        elif _type == TSDataType.DOUBLE:
            return [0.844421 for _ in range(_row)]
        elif _type == TSDataType.TEXT:
            return [
                "".join(random.choice(CHAR_BASE) for _ in range(5)) for _ in range(_row)
            ]
        else:
            raise TypeError("not support type:" + str(_type))

    values = {
        TIME_STR: pd.Series(
            np.arange(_row), dtype=FORMAT_CHAR_OF_TYPES[TSDataType.INT64]
        )
    }
    for column, data_type in measure_tstype_infos.items():
        values[column] = pd.Series(
            generate_data(data_type), dtype=FORMAT_CHAR_OF_TYPES[data_type]
        )

    df = pd.DataFrame(values)
    df.to_csv(data_file_name, index=False)
    print("data file has generated.")


def create_open_session():
    """
    creating session connection.
    :return:
    """
    ip = "127.0.0.1"
    port_ = "6667"
    username_ = "root"
    password_ = "root"
    session = Session(ip, port_, username_, password_, fetch_size=1024, zone_id="UTC+8")
    session.open(False)
    return session


def check_count(expect, _session, _sql):
    """
    check out the line number of the given SQL's query result.
    :param expect: expected number
    :param _session: IoTDB session
    :param _sql: query SQL
    """
    session_data_set = _session.execute_query_statement(_sql)
    session_data_set.set_fetch_size(1)
    get_count_line = False
    while session_data_set.has_next():
        if get_count_line:
            assert False, "select count return more than one line"
        line = session_data_set.next()
        actual = line.get_fields()[0].get_long_value()
        assert (
            expect == actual
        ), f"count error: expect {expect} lines, actual {actual} lines"
        get_count_line = True
    if not get_count_line:
        assert False, "select count has no result"
    session_data_set.close_operation_handle()


def check_query_result(expect, _session, _sql):
    """
    check out the query result of given query.
    :param expect: expected result
    :param _session: IoTDB session
    :param _sql: query SQL
    """
    session_data_set = _session.execute_query_statement(_sql)
    session_data_set.set_fetch_size(1)
    idx = 0
    while session_data_set.has_next():
        line = session_data_set.next()
        assert (
            str(line) == expect[idx]
        ), f"line {idx}: actual {str(line)} != expect ({expect[idx]})"
        idx += 1
    assert idx == len(expect), f"result rows: actual ({idx}) != expect ({len(expect)})"
    session_data_set.close_operation_handle()


def performance_test(
    measure_tstype_infos,
    data_file_name,
    use_new=True,
    check_result=False,
    row=10000,
    col=5000,
):
    """
    execute tablet insert using original or new methods.
    :param measure_tstype_infos: key(str): measurement name, value(TSDataType): measurement data type
    :param use_new: True if check out the result
    :param data_file_name: the csv file name to insert
    :param row: tablet row number
    :param col: tablet column number
    """
    print(
        f"Test python: use new: {use_new}, row: {row}, col: {col}. measurements: {measure_tstype_infos}"
    )
    print(f"Total points: {len(measure_tstype_infos) * row * col}")

    # open the session and clean data
    session = create_open_session()
    session.execute_non_query_statement("delete timeseries root.*")

    # test start
    st = time.perf_counter()
    csv_data = load_csv_data(measure_tstype_infos, data_file_name)
    load_cost = time.perf_counter() - st
    insert_cost = 0
    measurements = list(measure_tstype_infos.keys())
    data_types = list(measure_tstype_infos.values())
    for i in range(0, col):
        # if i % 500 == 0:
        #     print(f"insert {i} cols")
        device_id = "root.sg%d.%d" % (i % 8, i)
        if not use_new:
            # Use the ORIGINAL method to construct tablet
            timestamps_ = []
            values = []
            for t in range(0, row):
                timestamps_.append(csv_data.at[t, TIME_STR])
                value_array = []
                for m in measurements:
                    value_array.append(csv_data.at[t, m])
                values.append(value_array)
            tablet = Tablet(device_id, measurements, data_types, values, timestamps_)
        else:
            # Use the NEW method to construct numpy tablet
            timestamps_ = csv_data[TIME_STR].values
            if timestamps_.dtype != FORMAT_CHAR_OF_TYPES[TSDataType.INT64]:
                timestamps_ = timestamps_.astype(FORMAT_CHAR_OF_TYPES[TSDataType.INT64])
            values = []
            for measure, tstype in measure_tstype_infos.items():
                type_char = FORMAT_CHAR_OF_TYPES[tstype]
                value_array = csv_data[measure].values
                if value_array.dtype != type_char:
                    if not (tstype == TSDataType.TEXT and value_array.dtype == object):
                        value_array = value_array.astype(type_char)
                values.append(value_array)
            tablet = NumpyTablet(
                device_id, measurements, data_types, values, timestamps_
            )
        cost_st = time.perf_counter()
        session.insert_tablet(tablet)
        insert_cost += time.perf_counter() - cost_st

        if check_result:
            check_count(row, session, "select count(*) from %s" % device_id)
            expect = []
            for t in range(row):
                line = [str(csv_data.at[t, TIME_STR])]
                for m in measurements:
                    line.append(str(csv_data.at[t, m]))
                expect.append("\t\t".join([v for v in line]))
            check_query_result(
                expect, session, f"select {','.join(measurements)} from {device_id}"
            )
            print("query validation have passed")
    end = time.perf_counter()

    # clean data and close the session
    session.execute_non_query_statement("delete timeseries root.*")
    session.close()

    print("load cost: %.3f s" % load_cost)
    print("construct tablet cost: %.3f s" % (end - st - insert_cost - load_cost))
    print("insert tablet cost: %.3f s" % insert_cost)
    print("total cost: %.3f s" % (end - st))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="tablet performance comparison")
    parser.add_argument(
        "--row", type=int, default=10000, help="the row number of the input tablet"
    )
    parser.add_argument(
        "--col", type=int, default=5000, help="the column number of the input tablet"
    )
    parser.add_argument(
        "--check_result", "-c", action="store_true", help="True if check out the result"
    )
    parser.add_argument(
        "--use_new",
        "-n",
        action="store_false",
        help="True if use the new tablet insert",
    )
    parser.add_argument(
        "--seed", type=int, default=0, help="the random seed for generating csv data"
    )
    parser.add_argument(
        "--data_file_name", type=str, default="sample.csv", help="the path of csv data"
    )
    args = parser.parse_args()

    measure_tstype_infos = {
        "s0": TSDataType.BOOLEAN,
        "s1": TSDataType.FLOAT,
        "s2": TSDataType.INT32,
        "s3": TSDataType.DOUBLE,
        "s4": TSDataType.INT64,
        "s5": TSDataType.TEXT,
    }
    # if not os.path.exists(args.data_file_name):
    random.seed(a=args.seed, version=2)
    generate_csv_data(measure_tstype_infos, args.data_file_name, args.row, args.seed)

    performance_test(
        measure_tstype_infos,
        data_file_name=args.data_file_name,
        use_new=args.use_new,
        check_result=args.check_result,
        row=args.row,
        col=args.col,
    )
