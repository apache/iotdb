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

import random
import time
import numpy as np
import pandas as pd
from iotdb.mlnode.serde import convert_to_df
from pandas.testing import assert_frame_equal

device_id = "root.wt1"

ts_path_lst = [
    "root.wt1.temperature",
    "root.wt1.windspeed",
    "root.wt1.angle",
    "root.wt1.altitude",
    "root.wt1.status",
    "root.wt1.hardware",
]
measurements = [
    "temperature",
    "windspeed",
    "angle",
    "altitude",
    "status",
    "hardware",
]

simple_binary = [
    b'\x00\x00\x00\x06\x04\x03\x05\x00\x02\x01\x00\x00\x00\x14\x02\x02\x01\x03\x00\x02\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x02\x00\x00\x00\x00\x00\x00\x00\x03\x00\x00\x00\x00\x00\x00\x00\x04\x00\x00\x00\x00\x00\x00\x00\x05\x00\x00\x00\x00\x00\x00\x00\x06\x00\x00\x00\x00\x00\x00\x00\x07\x00\x00\x00\x00\x00\x00\x00\x08\x00\x00\x00\x00\x00\x00\x00\t\x00\x00\x00\x00\x00\x00\x00\n\x00\x00\x00\x00\x00\x00\x00\x0b\x00\x00\x00\x00\x00\x00\x00\x0c\x00\x00\x00\x00\x00\x00\x00\r\x00\x00\x00\x00\x00\x00\x00\x0e\x00\x00\x00\x00\x00\x00\x00\x0f\x00\x00\x00\x00\x00\x00\x00\x10\x00\x00\x00\x00\x00\x00\x00\x11\x00\x00\x00\x00\x00\x00\x00\x12\x00\x00\x00\x00\x00\x00\x00\x13\x00?\xc2\xfd\xe3\x85\xa0\xe9D?\xd2\x93^\xf7\xb2(\xb2?\xe4$\x8cf\xd6"\xe2?\xe9L\xdc\x0b\xd9\xfd\xe3?\xb5+57\xf01V?\xed\xf0\xae\xc9\x81\x93\xbe?\xde\xc7\x8fK7\x0b\x04?\xdc\x88\xd0h\xe3\x99B?\xed\x94\x1c\x1c\x15_c?\xe0\xe4g\xcepe\xef?\xde$\xde\x10\x96\xfc\x05?\x95\x9b\xddk\xabt\xb8?\xd8\x1a3\xe8\x8f\xcb\xe5?\xd8\x14\x0c\xd2Kf\xdc?\xd4A\x83xE\x0b"?\xeb\xb41\xa5\xbfl\xbd?\xdf\'\xa0-\x06\x9eU?\xcb\xcc_\xaa\t\xa9L?\xd1\xc5s1z\xf7B?\xea\xab\xdc\x16\xc1\xb8r\x00>\xc6\xcf\xca>\x08\xc1\xc6=\xc11\x97>\xa0&7?W\x14\x0b>\x94o\x97=\x8c\xad\x05>\xed2\x96>Bgg?nX:?1t\xac?\x13\xb6\xe1?I\x82*?6\xfb\x08?Q\xb0j>\x08K5?n\xd8!?7\xe2\xc1>\xdcG@?^x\x9d\x00\x00\x00\x00\x05text1\x00\x00\x00\x05text2\x00\x00\x00\x05text2\x00\x00\x00\x05text1\x00\x00\x00\x05text1\x00\x00\x00\x05text1\x00\x00\x00\x05text2\x00\x00\x00\x05text2\x00\x00\x00\x05text2\x00\x00\x00\x05text1\x00\x00\x00\x05text1\x00\x00\x00\x05text1\x00\x00\x00\x05text2\x00\x00\x00\x05text2\x00\x00\x00\x05text1\x00\x00\x00\x05text2\x00\x00\x00\x05text1\x00\x00\x00\x05text1\x00\x00\x00\x05text1\x00\x00\x00\x05text1\x00[\xd0\xe0\x00\x00\x00\x00\x00\x00\x00\x00\\\x00\x00\x00\x00\x00\x00\x00W\x00\x00\x00\x00\x00\x00\x00\r\x00\x00\x00\x00\x00\x00\x00S\x00\x00\x00\x00\x00\x00\x00G\x00\x00\x00\x00\x00\x00\x00@\x00\x00\x00\x00\x00\x00\x00S\x00\x00\x00\x00\x00\x00\x00>\x00\x00\x00\x00\x00\x00\x00\x11\x00\x00\x00\x00\x00\x00\x00P\x00\x00\x00\x00\x00\x00\x00\x17\x00\x00\x00\x00\x00\x00\x00\x1c\x00\x00\x00\x00\x00\x00\x00]\x00\x00\x00\x00\x00\x00\x00\x1e\x00\x00\x00\x00\x00\x00\x00\x15\x00\x00\x00\x00\x00\x00\x00V\x00\x00\x00\x00\x00\x00\x00Q\x00\x00\x00\x00\x00\x00\x00-\x00\x00\x00\x00\x00\x00\x00>\x00\x00\x00\x00\x00\x00\x00Y\x00\x00\x00\x00]\x00\x00\x00@\x00\x00\x00/\x00\x00\x00\x17\x00\x00\x00P\x00\x00\x00O\x00\x00\x00\x0f\x00\x00\x00+\x00\x00\x00O\x00\x00\x00\x12\x00\x00\x00\x11\x00\x00\x00:\x00\x00\x00L\x00\x00\x00-\x00\x00\x00\x1b\x00\x00\x00W\x00\x00\x00[\x00\x00\x00\x19\x00\x00\x00H\x00\x00\x00_']
binary_with_null = \
    [
        b'\x00\x00\x00\x06\x04\x03\x05\x00\x02\x01\x00\x00\x00\x13\x02\x02\x01\x03\x00\x02\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x02\x00\x00\x00\x00\x00\x00\x00\x03\x00\x00\x00\x00\x00\x00\x00\x04\x00\x00\x00\x00\x00\x00\x00\x05\x00\x00\x00\x00\x00\x00\x00\x06\x00\x00\x00\x00\x00\x00\x00\x07\x00\x00\x00\x00\x00\x00\x00\x08\x00\x00\x00\x00\x00\x00\x00\t\x00\x00\x00\x00\x00\x00\x00\n\x00\x00\x00\x00\x00\x00\x00\x0b\x00\x00\x00\x00\x00\x00\x00\x0c\x00\x00\x00\x00\x00\x00\x00\r\x00\x00\x00\x00\x00\x00\x00\x0e\x00\x00\x00\x00\x00\x00\x00\x0f\x00\x00\x00\x00\x00\x00\x00\x10\x00\x00\x00\x00\x00\x00\x00\x11\x00\x00\x00\x00\x00\x00\x00\x12\x01W4\xc0?\xe0\x07z\xef[\x18\xd0?\xd1=j\xa5l/\x8a?\xd4,<\x0ex\xb9\xf0?\xea\xfb\x1c~\x99Nq?\xe2\xe3\xce\xf3y\xee\xf4?\xc0\xd6\x03\xc0\x8b\x19,?\xe8\x15\x91\x0f!t\xf5?\xed\xc7%@JuA?\xea#OkLX,\x011\x11\x00=IC7>\xf6~\x18>\xa4P\x89>\xa0\xee\x08<\xf6\xe8\xd2<\xe1\xaa\xa0>\x89\xa1~?v4Q>\x9eK\xda>\xe6\x1e)>\xd4F_?-\x17c?*\x01\x8f>\xe8\t+\x01G\xdc\xa0\x00\x00\x00\x05text1\x00\x00\x00\x05text1\x00\x00\x00\x05text2\x00\x00\x00\x05text1\x00\x00\x00\x05text1\x00\x00\x00\x05text2\x00\x00\x00\x05text2\x00\x00\x00\x05text2\x01&\xec@\x88\x02\x00\x01\xee\xb6\xa0\x00\x00\x00\x00\x00\x00\x00B\x00\x00\x00\x00\x00\x00\x00\x15\x00\x00\x00\x00\x00\x00\x00\x0e\x00\x00\x00\x00\x00\x00\x00V\x00\x00\x00\x00\x00\x00\x00>\x00\x00\x00\x00\x00\x00\x00#\x01r[\x00\x00\x00\x00[\x00\x00\x00\x0b\x00\x00\x00T\x00\x00\x00P\x00\x00\x00W\x00\x00\x00\x11\x00\x00\x00)\x00\x00\x00<\x00\x00\x00\x0e\x00\x00\x00\x18',
        b'\x00\x00\x00\x06\x04\x03\x05\x00\x02\x01\x00\x00\x00\x01\x02\x02\x01\x03\x04\x02\x04\x00\x00\x00\x00\x00\x00\x00\x00\x13\x00?\xd3\xc7\xae#\x86\xe1j\x00?\x10\xea!\x00\x00\x00\x00\x05text1\x00\x01\x80\x00\x00\x00\x00\x00\x00\x00\x00\x00?\x01\x01\x80']
column_names = ['root.wt1.altitude', 'root.wt1.temperature', 'root.wt1.angle', 'root.wt1.windspeed',
                'root.wt1.hardware', 'root.wt1.status']
data_type_list = ['INT64', 'FLOAT', 'INT32', 'DOUBLE', 'TEXT', 'BOOLEAN']
column_name_index = {'root.wt1.windspeed': 0, 'root.wt1.temperature': 1, 'root.wt1.hardware': 2, 'root.wt1.status': 3,
                     'root.wt1.altitude': 4, 'root.wt1.angle': 5}


def test_simple_query():
    # test data
    data_nums = 20
    data = {}
    timestamps = np.arange(data_nums, dtype="int64")
    data[ts_path_lst[0]] = \
        [0.38830405, 0.13355169, 0.09433287, 0.31279156, 0.8401496,
         0.28991386, 0.06868938, 0.46327657, 0.18984757, 0.93103373,
         0.6931865, 0.57700926, 0.7871424, 0.71476793, 0.8190981,
         0.13309939, 0.93298537, 0.71830374, 0.4302311, 0.8690279]
    data[ts_path_lst[1]] = \
        [0.14837307, 0.29024481, 0.62946148, 0.79063227, 0.08269055,
         0.93563022, 0.48093016, 0.44585047, 0.92432981, 0.52788153,
         0.47100021, 0.02110239, 0.37659929, 0.37622376, 0.31649863,
         0.86574633, 0.48679356, 0.21717449, 0.27767639, 0.83347897]
    data[ts_path_lst[2]] = \
        [93, 64, 47, 23, 80, 79, 15, 43, 79, 18, 17, 58, 76, 45, 27, 87, 91, 25, 72, 95]
    data[ts_path_lst[3]] = \
        [92, 87, 13, 83, 71, 64, 83, 62, 17, 80, 23, 28, 93, 30, 21, 86, 81,
         45, 62, 89]
    data[ts_path_lst[4]] = [False, True, False, True, True, False, True, True, True,
                            True, False, True, False, False, False, False, True, True,
                            True, False]
    data[ts_path_lst[5]] = ['text1', 'text2', 'text2', 'text1', 'text1', 'text1', 'text2',
                            'text2', 'text2', 'text1', 'text1', 'text1', 'text2', 'text2',
                            'text1', 'text2', 'text1', 'text1', 'text1', 'text1']

    data[ts_path_lst[0]] = np.array(data[ts_path_lst[0]], dtype="float32")
    data[ts_path_lst[2]] = np.array(data[ts_path_lst[2]], dtype="int32")

    df_input = pd.DataFrame(data)
    df_input.insert(0, "Time", timestamps)

    df_output = convert_to_df(column_names, data_type_list, column_name_index, simple_binary)
    df_output = df_output[df_input.columns.tolist()]

    assert_frame_equal(df_input, df_output)


def test_with_null_query():
    # insert data

    data_nums = 20
    data = {}
    timestamps = np.arange(data_nums, dtype="int64")
    data[ts_path_lst[0]] = [0.049136366695165634, 0.4814307689666748, None, None, 0.3209269344806671,
                            0.3143160343170166, 0.030140314251184464, None, 0.027547180652618408, 0.2688102126121521,
                            0.9617357850074768, None, 0.30917245149612427, 0.4494488537311554, 0.41459938883781433,
                            None, 0.6761381030082703, 0.6640862822532654, 0.4531949460506439, 0.5660725235939026]
    data[ts_path_lst[1]] = [0.5009131121558088, None, 0.26937357096243686, None, 0.3151998654674619, None, None, None,
                            0.8431532356866694, 0.5903086429020434, None, None, 0.1315312090066042, None,
                            0.7526326461335474, 0.9305597549133965, None, None, 0.8168103309315078, 0.30906251401349627]
    data[ts_path_lst[2]] = [91, None, None, None, 11, 84, None, 80, 87, None, 17, None, None, 41, None, None, 60, 14,
                            24, None]
    data[ts_path_lst[3]] = [None, None, None, 66, None, None, None, 21, None, 14, None, None, 86, None, None, 62, None,
                            35, None, 63]
    data[ts_path_lst[4]] = [True, False, None, False, True, None, None, False, None, None, None, False, None, None,
                            True, False, False, None, False, None]
    data[ts_path_lst[5]] = ['text1', None, 'text1', 'text2', 'text1', None, None, None, None, None, 'text1', None, None,
                            None, 'text2', 'text2', None, 'text2', None, 'text1']

    data[ts_path_lst[0]] = pd.Series(data[ts_path_lst[0]]).astype("float32")
    data[ts_path_lst[2]] = pd.Series(data[ts_path_lst[2]]).astype("Int32")
    data[ts_path_lst[3]] = pd.Series(data[ts_path_lst[3]]).astype("Int64")
    data[ts_path_lst[4]] = pd.Series(data[ts_path_lst[4]]).astype("boolean")

    df_input = pd.DataFrame(data)
    df_input.insert(0, "Time", timestamps)

    df_output = convert_to_df(column_names, data_type_list, column_name_index, binary_with_null)
    df_output = df_output[df_input.columns.tolist()]
    assert_frame_equal(df_input, df_output)
