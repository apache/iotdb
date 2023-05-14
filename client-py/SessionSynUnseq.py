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
#

from iotdb.Session import Session
from iotdb.utils.IoTDBConstants import TSDataType, TSEncoding, Compressor
from iotdb.utils.Tablet import Tablet
import pandas as pd
import numpy as np
import time

mu, sig = 2.0, 2.6
grpSuf = '_sig'+str(int(sig*10))

# creating session connection.
ip = "127.0.0.1"
port_ = "6667"
username_ = "root"
password_ = "root"
session = Session(ip, port_, username_, password_, fetch_size=1024, zone_id="UTC+8")
session.open(False)

grp = "bt_syn_01"+grpSuf

# set and delete storage groups
session.delete_storage_group("root." + grp)
session.set_storage_group("root." + grp)

# setting time series.
session.create_time_series(
    "root." + grp + ".d_01.s_01", TSDataType.DOUBLE, TSEncoding.PLAIN, Compressor.SNAPPY
)

# checking time series
print(
    "s_01 expecting True, checking result: ",
    session.check_time_series_exists("root." + grp + ".d_01.s_01"),
)

df = pd.read_csv("D:\\Study\\Lab\\iotdb\\add_quantile_to_aggregation\\test_project_2\\1_bitcoin.csv")
data = df["bitcoin dataset"].tolist()
# df = pd.read_csv("../../4_wh.csv")
# data = df["value"].tolist()
data = [[datum] for datum in data]
# df = pd.read_csv("../../SpacecraftThruster.txt")
# df = pd.read_csv("../../4_taxipredition8M.txt")
# data = (np.array(df)).tolist()
# data = [[datum[0]] for datum in data]
batch = 8192
print(data[:10])
print(type(data[0]))
print(len(data))

n=6713*8192
lat_data = []
import random
import math

random.seed(233)
for i in range(n):
    lat_data.append( ( int(i+math.exp(mu + sig * random.gauss(0, 1))), i, data[i] ) )
lat_data=sorted(lat_data,key=lambda x:x[0])


measurements_ = ["s_01"]
data_types_ = [
    TSDataType.DOUBLE
]

values_ = range(batch)
timestamps_ = range(1<<40,(1<<40)+batch)
tablet_ = Tablet(
    "root." + grp + ".d_01", measurements_, data_types_, values_, timestamps_
)
session.insert_tablet(tablet_)
session.execute_non_query_statement("flush")

total_time = 0
for i in range(int(n/batch)):
    if i % 100 == 0:
        print("Iter: " + str(i))
    # insert one tablet into the database.
    values_ = data[i * batch : (i + 1) * batch][1]
    timestamps_ = list(range(i * batch, (i + 1) * batch))
    values_ = []
    timestamps_ = []
    for j in range(i*batch,(i+1)*batch):
        timestamps_.append(lat_data[j][1])
        values_.append(lat_data[j][2])
        # print('\t\t'+str(lat_data[j][1]))
    if len(timestamps_) != len(values_):
        break
    tablet_ = Tablet(
        "root." + grp + ".d_01", measurements_, data_types_, values_, timestamps_
    )
    curr_time = time.time()
    session.insert_tablet(tablet_)
    total_time += (time.time() - curr_time)

print(total_time)
#
#
# # --------------------------------------------------------------------------------------------------------------------
#
# grp = "tx_syn_01"
#
# # set and delete storage groups
# session.delete_storage_group("root." + grp)
# session.set_storage_group("root." + grp)
#
# # setting time series.
# session.create_time_series(
#     "root." + grp + ".d_01.s_01", TSDataType.DOUBLE, TSEncoding.PLAIN, Compressor.SNAPPY
# )
#
# # checking time series
# print(
#     "s_01 expecting True, checking result: ",
#     session.check_time_series_exists("root." + grp + ".d_01.s_01"),
# )
#
# # df = pd.read_csv("../../1_bitcoin.csv")
# # data = df["bitcoin dataset"].tolist()
# # df = pd.read_csv("../../4_wh.csv")
# # data = df["value"].tolist()
# # data = [[datum] for datum in data]
# # df = pd.read_csv("../../SpacecraftThruster.txt")
# df = pd.read_csv("../../4_taxipredition8M.txt")
# data = (np.array(df)).tolist()
# data = [[datum[0]] for datum in data]
# batch = 8192
# print(data[:10])
# print(type(data[0]))
# print(len(data))
#
# measurements_ = ["s_01"]
# data_types_ = [
#     TSDataType.DOUBLE
# ]
#
# total_time = 0
# for i in range(6713):
#     if i % 100 == 0:
#         print("Iter: " + str(i))
#     # insert one tablet into the database.
#     values_ = data[i * batch : (i + 1) * batch] # Non-ASCII text will cause error since bytes can only hold 0-128 nums.
#     timestamps_ = list(range(i * batch, (i + 1) * batch))
#     if len(timestamps_) != len(values_):
#         break
#     tablet_ = Tablet(
#         "root." + grp + ".d_01", measurements_, data_types_, values_, timestamps_
#     )
#     curr_time = time.time()
#     session.insert_tablet(tablet_)
#     total_time += (time.time() - curr_time)
#
# print(total_time)
#
# # --------------------------------------------------------------------------------------------------------------------
#
# grp = "th_syn_01"
#
# # set and delete storage groups
# session.delete_storage_group("root." + grp)
# session.set_storage_group("root." + grp)
#
# # setting time series.
# session.create_time_series(
#     "root." + grp + ".d_01.s_01", TSDataType.DOUBLE, TSEncoding.PLAIN, Compressor.SNAPPY
# )
#
# # checking time series
# print(
#     "s_01 expecting True, checking result: ",
#     session.check_time_series_exists("root." + grp + ".d_01.s_01"),
# )
#
# # df = pd.read_csv("../../1_bitcoin.csv")
# # data = df["bitcoin dataset"].tolist()
# # df = pd.read_csv("../../4_wh.csv")
# # data = df["value"].tolist()
# # data = [[datum] for datum in data]
# df = pd.read_csv("../../SpacecraftThruster.txt")
# # df = pd.read_csv("../../4_taxipredition8M.txt")
# data = (np.array(df)).tolist()
# data = [[datum[0]] for datum in data]
# batch = 8192
# print(data[:10])
# print(type(data[0]))
# print(len(data))
#
# measurements_ = ["s_01"]
# data_types_ = [
#     TSDataType.DOUBLE
# ]
#
# total_time = 0
# for i in range(6713):
#     if i % 100 == 0:
#         print("Iter: " + str(i))
#     # insert one tablet into the database.
#     values_ = data[i * batch : (i + 1) * batch] # Non-ASCII text will cause error since bytes can only hold 0-128 nums.
#     timestamps_ = list(range(i * batch, (i + 1) * batch))
#     if len(timestamps_) != len(values_):
#         break
#     tablet_ = Tablet(
#         "root." + grp + ".d_01", measurements_, data_types_, values_, timestamps_
#     )
#     curr_time = time.time()
#     session.insert_tablet(tablet_)
#     total_time += (time.time() - curr_time)
#
# print(total_time)
#
# # --------------------------------------------------------------------------------------------------------------------
#
# grp = "wh_syn_01"
#
# # set and delete storage groups
# session.delete_storage_group("root." + grp)
# session.set_storage_group("root." + grp)
#
# # setting time series.
# session.create_time_series(
#     "root." + grp + ".d_01.s_01", TSDataType.DOUBLE, TSEncoding.PLAIN, Compressor.SNAPPY
# )
#
# # checking time series
# print(
#     "s_01 expecting True, checking result: ",
#     session.check_time_series_exists("root." + grp + ".d_01.s_01"),
# )
#
# # df = pd.read_csv("../../1_bitcoin.csv")
# # data = df["bitcoin dataset"].tolist()
# df = pd.read_csv("../../4_wh.csv")
# data = df["value"].tolist()
# data = [[datum] for datum in data]
# # df = pd.read_csv("../../SpacecraftThruster.txt")
# # df = pd.read_csv("../../4_taxipredition8M.txt")
# # data = (np.array(df)).tolist()
# # data = [[datum[0]] for datum in data]
# batch = 8192
# print(data[:10])
# print(type(data[0]))
# print(len(data))
#
# measurements_ = ["s_01"]
# data_types_ = [
#     TSDataType.DOUBLE
# ]
#
# total_time = 0
# for i in range(6713):
#     if i % 100 == 0:
#         print("Iter: " + str(i))
#     # insert one tablet into the database.
#     values_ = data[i * batch : (i + 1) * batch] # Non-ASCII text will cause error since bytes can only hold 0-128 nums.
#     timestamps_ = list(range(i * batch, (i + 1) * batch))
#     if len(timestamps_) != len(values_):
#         break
#     tablet_ = Tablet(
#         "root." + grp + ".d_01", measurements_, data_types_, values_, timestamps_
#     )
#     curr_time = time.time()
#     session.insert_tablet(tablet_)
#     total_time += (time.time() - curr_time)
#
# print(total_time)

session.close()

print("All executions done!!")