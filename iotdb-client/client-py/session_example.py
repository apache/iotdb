from datetime import datetime
import numpy as np

from iotdb.Session import Session
from iotdb.utils.IoTDBConstants import TSDataType, TSEncoding, Compressor
from iotdb.utils.NumpyTablet import NumpyTablet

# creating session connection.
ip = "127.0.0.1"
port_ = "6667"
username_ = "root"
password_ = "root"
session = Session(
    ip,
    port_,
    username_,
    password_,
    fetch_size=5000,
    zone_id="UTC+8",
    enable_redirection=False,
)
session.open()
# startTime = int(datetime.now().timestamp() * 1000)
# with session.execute_query_statement("select ** from root") as data_set:
#     data_set.todf()
#
# print("todf cost: " + str(int(datetime.now().timestamp() * 1000) - startTime) + "ms")
startTime = int(datetime.now().timestamp() * 1000)
with session.execute_query_statement("select * from root.**") as data_set:
    data_set.get_column_names()
    data_set.get_column_types()
    while data_set.has_next():
        data_set.next()
        # print(data_set.next())
print("Query cost: " + str(int(datetime.now().timestamp() * 1000) - startTime) + "ms")
session.close()
exit(0)
