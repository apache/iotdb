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

grps = ["tx_syn_01", "tx_syn_02", "tx_syn_03", "tx_syn_04"]
# grps = ["bt_syn_01", "bt_syn_02", "bt_syn_03", "bt_syn_04"]
# grps = ["st_syn_01", "st_syn_02", "st_syn_03", "st_syn_04"]
# grps = ["wh_syn_01", "wh_syn_02", "wh_syn_03", "wh_syn_04"]
times = 6
drop = 3
data_size = 50000000
sizes = [10000000, 20000000, 30000000, 40000000, 50000000]
quant = 0.5

for grp in grps:
    query_times = 0
    accuracy = 0
    print(grp)
    for i in range(times):
        start_time = time.time()
        result = session.execute_query_statement(
            "select KLL_QUANTILE(s_01) from root." + grp + ".d_01"
        )
        if i < drop:
            continue
        query_times += time.time() - start_time
        # result = str(result.next()).split()
        # quantile = float(result[1])

        # count = session.execute_query_statement(
        #     "select count(s_01) from root." + grp + ".d_01 where s_01<=" + str(quantile)
        # )

        # count =str(count.next()).split()
        # accuracy += data_size * quant - float(count[1])

    print(query_times / (times - drop), (accuracy / (times - drop)) / data_size)

for size in sizes:
    print(size)
    query_times = 0
    accuracy = 0
    for i in range(times):
        start_time = time.time()
        result = session.execute_query_statement(
            "select KLL_QUANTILE(s_01) from root." + grps[0] + ".d_01 where time<=" + str(size)
        )
        if i < drop:
            continue
        query_times += time.time() - start_time
        # result = str(result.next()).split()
        # quantile = float(result[1])

        # count = session.execute_query_statement(
        #     # "select count(s_01) from root.sg_syn_02.d_01 where s_01<=" + str(quantile) + " and time<=" + str(size)
        #     "select count(s_01) from root.sg_td_02.d_01 where s_01<=" + str(quantile) + " and time<=" + str(size)
        #     # "select count(s_01) from root.sg_rs_02.d_01 where s_01<=" + str(quantile) + " and time<=" + str(size)
        # )

        # count =str(count.next()).split()
        # accuracy += size * quant - float(count[1])

    print(query_times / (times - drop), (accuracy / (times - drop)) / size)
