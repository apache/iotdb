from iotdb.Session import Session
from iotdb.utils.IoTDBConstants import TSDataType, TSEncoding, Compressor
from iotdb.utils.Tablet import Tablet
import pandas as pd
import numpy as np
import time

# creating session connection.
ip = "127.0.0.1"
port_ = "6667"
username_ = "root"
password_ = "root"
session = Session(ip, port_, username_, password_, fetch_size=1024, zone_id="UTC+8")
session.open(False)

# grps = ["tx_syn_01", "tx_syn_05"]
# grps = ["bt_syn_01", "bt_syn_05"]
# grps = ["st_syn_01", "st_syn_05"]
# grps = ["wh_syn_01", "wh_syn_05"]
ds = [["bt_syn_01", "bt_syn_05"], ["st_syn_01", "st_syn_05"], ["tx_syn_01", "tx_syn_05"], ["wh_syn_01", "wh_syn_05"]]
times = 10
drop = 0
data_size = 50000000

for grps in ds:
    res = []
    print(grps)
    for i in range(3):
        result = session.execute_query_statement(
                    # "select KLL_QUANTILE(s_01) from root." + grps[0] + ".d_01"
                    # "select TDIGEST_QUANTILE(s_01) from root." + grps[0] + ".d_01"
                    # "select SAMPLING_QUANTILE(s_01) from root." + grps[0] + ".d_01"
                )

    query_times = 0
    for i in range(times + 2):
        start_time = time.time()
        result = session.execute_query_statement(
            "select KLL_QUANTILE(s_01) from root." + grps[0] + ".d_01 where time<=2000000"
        )
        query_times += time.time() - start_time

    print(query_times / (times + 2 - drop))
    res.append(query_times / (times + 2 - drop))

    for i in range(3):
        result = session.execute_query_statement(
                    # "select KLL_QUANTILE(s_01) from root." + grps[0] + ".d_01"
                    # "select TDIGEST_QUANTILE(s_01) from root." + grps[0] + ".d_01"
                    # "select SAMPLING_QUANTILE(s_01) from root." + grps[0] + ".d_01"
                )

    query_times = 0
    for i in range(times):
        start_time = time.time()
        result = session.execute_query_statement(
            "select SAMPLING_QUANTILE(s_01) from root." + grps[1] + ".d_01 where time<=2000000"
        )
        query_times += time.time() - start_time

    print(query_times / (times - drop))
    res.append(query_times / (times - drop))

    query_times = 0
    for i in range(times):
        start_time = time.time()
        result = session.execute_query_statement(
            "select TDIGEST_QUANTILE(s_01) from root." + grps[1] + ".d_01 where time<=2000000"
        )
        query_times += time.time() - start_time

    print(query_times / (times - drop))
    res.append(query_times / (times - drop))

    query_times = 0
    for i in range(times + 2):
        start_time = time.time()
        result = session.execute_query_statement(
            "select KLL_QUANTILE(s_01) from root." + grps[1] + ".d_01 where time<=2000000"
        )
        query_times += time.time() - start_time

    print(query_times / (times + 2 - drop))
    res.append(query_times / (times + 2 - drop))

    print(str(int(res[0] * 1000)) + " " + str(int(res[1] * 1000)) + " " + str(int(res[2] * 1000)) + " " + str(int(res[3] * 1000)))
