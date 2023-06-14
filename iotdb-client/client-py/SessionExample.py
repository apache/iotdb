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

# Uncomment the following line to use apache-iotdb module installed by pip3
import numpy as np

from iotdb.Session import Session
from iotdb.template.MeasurementNode import MeasurementNode
from iotdb.template.Template import Template
from iotdb.utils.BitMap import BitMap
from iotdb.utils.IoTDBConstants import TSDataType, TSEncoding, Compressor
from iotdb.utils.Tablet import Tablet
from iotdb.utils.NumpyTablet import NumpyTablet

# creating session connection.
ip = "127.0.0.1"
port_ = "6667"
username_ = "root"
password_ = "root"
# session = Session(ip, port_, username_, password_, fetch_size=1024, zone_id="UTC+8", enable_redirection=True)
session = Session.init_from_node_urls(
    node_urls=["127.0.0.1:666", "127.0.0.1:6668", "127.0.0.1:6669"],
    user="root",
    password="root",
    fetch_size=1024,
    zone_id="UTC+8",
    enable_redirection=True,
)
session.open(False)
session.execute_raw_data_query(["root.sg.d1.s1"], None, None)

# close session connection.
session.close()

print("All executions done!!")
