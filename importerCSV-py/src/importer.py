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

import sys
import struct
from datetime import datetime

from Session import Session
from IoTDBConstants import *
from SessionDataSet import SessionDataSet

from thrift.protocol import TBinaryProtocol, TCompactProtocol
from thrift.transport import TSocket, TTransport

import csv


class Importer:
    def __init__(self, session: Session):
        self.session = session

    def align_all_series(self, file, time_format='%Y-%m-%dT%H:%M:%S.%f%z', sg=None):
        self.session.open(False)
        try:
            csvFile = open(file, "r")
            reader = csv.reader(csvFile)
            deviceID_lst = []
            measurement_lst = []
            for line in reader:
                num_of_series = len(line) - 1
                if reader.line_num == 1:
                    for item in line:
                        if item != 'Time':
                            deviceID_lst.append('.'.join(item.split('.')[:-1]))
                            measurement_lst.append(item.split('.')[-1])
                else:
                    time = self.__time_to_timestamp(line[0], time_format)
                    for i in range(num_of_series):
                        if line[i + 1] not in ('', ' ', None, "null", "Null"):
                            if sg:
                                deviceID = sg + "." + deviceID_lst[i]
                            else:
                                deviceID = deviceID_lst[i]
                            self.session.insert_str_record(deviceID, time,
                                                           [measurement_lst[i]],
                                                           [line[i + 1]])
            csvFile.close()
        except Exception:
            print("the csv format is incorrect")
        self.session.close()

    def align_by_device(self, file, time_format='%Y-%m-%dT%H:%M:%S.%f%z', sg=None):
        self.session.open(False)
        try:
            csvFile = open(file, "r")
            reader = csv.reader(csvFile)
            measurement_lst = []
            for line in reader:
                num_of_series = len(line) - 2
                if reader.line_num == 1:
                    for item in line:
                        if item != 'Time' and item != 'Device':
                            measurement_lst.append(item)
                else:
                    time = self.__time_to_timestamp(line[0], time_format)
                    if sg:
                        deviceID = sg + "." + line[1]
                    else:
                        deviceID = line[1]
                    for i in range(num_of_series):
                        if line[i + 2] not in ('', ' ', None, "null", "Null"):
                            self.session.insert_str_record(deviceID, time,
                                                           [measurement_lst[i]],
                                                           [line[i + 2]])
            csvFile.close()
        except Exception:
            print("the csv format is incorrect")
        self.session.close()

    @staticmethod
    def __time_to_timestamp(str_time: str, time_format: str):
        """str_time: the string representation of date and time with timezone
         at the end.
        e.g. '2012-11-01T04:16:13-04:00'

        time_format: the time format written with format tokens and included
        the time zone at the end
        e.g. '%Y-%m-%dT%H:%M:%S%z'
        """
        try:
            return int(str_time)
        except TypeError:
            time = datetime.strptime(''.join(str_time.rsplit(':', 1)), time_format)
            timestamp = int(datetime.timestamp(time))
            return timestamp
