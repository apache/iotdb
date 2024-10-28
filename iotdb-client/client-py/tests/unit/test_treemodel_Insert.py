import numpy as np
import pytest
import yaml
import os
from datetime import date
from iotdb.Session import Session
from iotdb.SessionPool import SessionPool, PoolConfig
from iotdb.utils.BitMap import BitMap
from iotdb.utils.IoTDBConstants import TSDataType, TSEncoding, Compressor
from iotdb.utils.Tablet import Tablet
from iotdb.utils.NumpyTablet import NumpyTablet
from datetime import date

# Perform some setup before a test case is run
ip = "127.0.0.1"
port_ = "6667"
username_ = "root"
password_ = "root"
session = None 

def get_session():
    global session
    session = Session(ip, port_, username_, password_)
    session.open()
    return session

def create_database(session):
    session.set_storage_group("root.test.g1")
    session.set_storage_group("root.test.g2")
    session.set_storage_group("root.test.g3")
    session.set_storage_group("root.test.g4")
    session.set_storage_group("root.test.g5")
    session.set_storage_group("root.test.g6")

def create_timeseries(session):
    # 1、Create a single time series
    session.create_time_series("root.test.g5.d1.STRING1", TSDataType.STRING, TSEncoding.DICTIONARY, Compressor.UNCOMPRESSED,
        props=None, tags=None, attributes=None, alias=None)
    session.create_time_series("root.test.g5.d1.STRING2", TSDataType.STRING, TSEncoding.PLAIN, Compressor.LZ4,
        props=None, tags=None, attributes=None, alias=None)
    session.create_time_series("root.test.g5.d1.STRING3", TSDataType.STRING, TSEncoding.DICTIONARY, Compressor.GZIP,
        props=None, tags=None, attributes=None, alias=None)
    session.create_time_series("root.test.g5.d1.STRING4", TSDataType.STRING, TSEncoding.PLAIN, Compressor.ZSTD,
        props=None, tags=None, attributes=None, alias=None)
    session.create_time_series("root.test.g5.d1.STRING5", TSDataType.STRING, TSEncoding.DICTIONARY, Compressor.LZMA2,
        props=None, tags=None, attributes=None, alias=None)
    session.create_time_series("root.test.g5.d1.STRING6", TSDataType.STRING, TSEncoding.PLAIN, Compressor.LZ4,
        props=None, tags=None, attributes=None, alias=None)
    
    session.create_time_series("root.test.g5.d1.TS1", TSDataType.TIMESTAMP, TSEncoding.PLAIN, Compressor.GZIP,
        props=None, tags=None, attributes=None, alias=None)
    session.create_time_series("root.test.g5.d1.TS2", TSDataType.TIMESTAMP, TSEncoding.RLE, Compressor.UNCOMPRESSED,
        props=None, tags=None, attributes=None, alias=None)
    session.create_time_series("root.test.g5.d1.TS3", TSDataType.TIMESTAMP, TSEncoding.TS_2DIFF, Compressor.LZ4,
        props=None, tags=None, attributes=None, alias=None)
    session.create_time_series("root.test.g5.d1.TS4", TSDataType.TIMESTAMP, TSEncoding.ZIGZAG, Compressor.ZSTD,
        props=None, tags=None, attributes=None, alias=None)
    session.create_time_series("root.test.g5.d1.TS5", TSDataType.TIMESTAMP, TSEncoding.CHIMP, Compressor.LZMA2,
        props=None, tags=None, attributes=None, alias=None)
    session.create_time_series("root.test.g5.d1.TS6", TSDataType.TIMESTAMP, TSEncoding.SPRINTZ, Compressor.GZIP,
        props=None, tags=None, attributes=None, alias=None)
    session.create_time_series("root.test.g5.d1.TS7", TSDataType.TIMESTAMP, TSEncoding.RLBE, Compressor.LZ4,
        props=None, tags=None, attributes=None, alias=None)
    session.create_time_series("root.test.g5.d1.TS8", TSDataType.TIMESTAMP, TSEncoding.GORILLA, Compressor.GZIP,
        props=None, tags=None, attributes=None, alias=None)
    
    session.create_time_series("root.test.g5.d1.DATE1", TSDataType.DATE, TSEncoding.PLAIN, Compressor.GZIP,
        props=None, tags=None, attributes=None, alias=None)
    session.create_time_series("root.test.g5.d1.DATE2", TSDataType.DATE, TSEncoding.RLE, Compressor.UNCOMPRESSED,
        props=None, tags=None, attributes=None, alias=None)
    session.create_time_series("root.test.g5.d1.DATE3", TSDataType.DATE, TSEncoding.TS_2DIFF, Compressor.LZ4,
        props=None, tags=None, attributes=None, alias=None)
    session.create_time_series("root.test.g5.d1.DATE4", TSDataType.DATE, TSEncoding.ZIGZAG, Compressor.ZSTD,
        props=None, tags=None, attributes=None, alias=None)
    session.create_time_series("root.test.g5.d1.DATE5", TSDataType.DATE, TSEncoding.CHIMP, Compressor.LZMA2,
        props=None, tags=None, attributes=None, alias=None)
    session.create_time_series("root.test.g5.d1.DATE6", TSDataType.DATE, TSEncoding.SPRINTZ, Compressor.GZIP,
        props=None, tags=None, attributes=None, alias=None)
    session.create_time_series("root.test.g5.d1.DATE7", TSDataType.DATE, TSEncoding.RLBE, Compressor.LZ4,
        props=None, tags=None, attributes=None, alias=None)
    session.create_time_series("root.test.g5.d1.DATE8", TSDataType.DATE, TSEncoding.GORILLA, Compressor.GZIP,
        props=None, tags=None, attributes=None, alias=None)
    
    session.create_time_series("root.test.g5.d1.BLOB1", TSDataType.BLOB, TSEncoding.PLAIN, Compressor.UNCOMPRESSED,
        props=None, tags=None, attributes=None, alias=None)
    session.create_time_series("root.test.g5.d1.BLOB2", TSDataType.BLOB, TSEncoding.PLAIN, Compressor.LZ4,
        props=None, tags=None, attributes=None, alias=None)
    session.create_time_series("root.test.g5.d1.BLOB3", TSDataType.BLOB, TSEncoding.PLAIN, Compressor.GZIP,
        props=None, tags=None, attributes=None, alias=None)
    session.create_time_series("root.test.g5.d1.BLOB4", TSDataType.BLOB, TSEncoding.PLAIN, Compressor.ZSTD,
        props=None, tags=None, attributes=None, alias=None)
    session.create_time_series("root.test.g5.d1.BLOB5", TSDataType.BLOB, TSEncoding.PLAIN, Compressor.LZMA2,
        props=None, tags=None, attributes=None, alias=None)
    session.create_time_series("root.test.g5.d1.BLOB6", TSDataType.BLOB, TSEncoding.PLAIN, Compressor.LZ4,
        props=None, tags=None, attributes=None, alias=None)

    
    # 2、Create multiple time series
    ts_path_lst = ["root.test.g1.d1.BOOLEAN", "root.test.g1.d1.INT32", "root.test.g1.d1.INT64", "root.test.g1.d1.FLOAT", "root.test.g1.d1.DOUBLE", "root.test.g1.d1.TEXT", "root.test.g1.d1.TS", "root.test.g1.d1.DATE", "root.test.g1.d1.BLOB", "root.test.g1.d1.STRING",
                   "root.test.g1.d2.BOOLEAN", "root.test.g1.d2.INT32", "root.test.g1.d2.INT64", "root.test.g1.d2.FLOAT", "root.test.g1.d2.DOUBLE", "root.test.g1.d2.TEXT", "root.test.g1.d2.TS", "root.test.g1.d2.DATE", "root.test.g1.d2.BLOB", "root.test.g1.d2.STRING",
                   "root.test.g1.d3.BOOLEAN", "root.test.g1.d3.INT32", "root.test.g1.d3.INT64", "root.test.g1.d3.FLOAT", "root.test.g1.d3.DOUBLE", "root.test.g1.d3.TEXT", "root.test.g1.d3.TS", "root.test.g1.d3.DATE", "root.test.g1.d3.BLOB", "root.test.g1.d3.STRING",
                   "root.test.g2.d1.BOOLEAN", "root.test.g2.d1.INT32", "root.test.g2.d1.INT64", "root.test.g2.d1.FLOAT", "root.test.g2.d1.DOUBLE", "root.test.g2.d1.TEXT", "root.test.g2.d1.TS", "root.test.g2.d1.DATE", "root.test.g2.d1.BLOB", "root.test.g2.d1.STRING"]
    data_type_lst = [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE, TSDataType.TEXT, TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB, TSDataType.STRING,
                     TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE, TSDataType.TEXT, TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB, TSDataType.STRING,
                     TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE, TSDataType.TEXT, TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB, TSDataType.STRING,
                     TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE, TSDataType.TEXT, TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB, TSDataType.STRING]
    encoding_lst = [TSEncoding.RLE, TSEncoding.CHIMP, TSEncoding.ZIGZAG, TSEncoding.RLBE, TSEncoding.SPRINTZ, TSEncoding.DICTIONARY, TSEncoding.TS_2DIFF, TSEncoding.CHIMP, TSEncoding.PLAIN, TSEncoding.DICTIONARY,
                    TSEncoding.RLE, TSEncoding.CHIMP, TSEncoding.ZIGZAG, TSEncoding.RLBE, TSEncoding.SPRINTZ, TSEncoding.DICTIONARY, TSEncoding.TS_2DIFF, TSEncoding.CHIMP, TSEncoding.PLAIN, TSEncoding.DICTIONARY,
                    TSEncoding.RLE, TSEncoding.CHIMP, TSEncoding.ZIGZAG, TSEncoding.RLBE, TSEncoding.SPRINTZ, TSEncoding.DICTIONARY, TSEncoding.TS_2DIFF, TSEncoding.CHIMP, TSEncoding.PLAIN, TSEncoding.DICTIONARY,
                    TSEncoding.RLE, TSEncoding.CHIMP, TSEncoding.ZIGZAG, TSEncoding.RLBE, TSEncoding.SPRINTZ, TSEncoding.DICTIONARY, TSEncoding.TS_2DIFF, TSEncoding.CHIMP, TSEncoding.PLAIN, TSEncoding.DICTIONARY]
    compressor_lst = [Compressor.UNCOMPRESSED, Compressor.SNAPPY, Compressor.LZ4, Compressor.GZIP, Compressor.ZSTD, Compressor.LZMA2, Compressor.GZIP, Compressor.GZIP, Compressor.GZIP,  Compressor.GZIP,
                      Compressor.UNCOMPRESSED, Compressor.SNAPPY, Compressor.LZ4, Compressor.GZIP, Compressor.ZSTD, Compressor.LZMA2, Compressor.GZIP, Compressor.GZIP, Compressor.GZIP,  Compressor.GZIP,
                      Compressor.UNCOMPRESSED, Compressor.SNAPPY, Compressor.LZ4, Compressor.GZIP, Compressor.ZSTD, Compressor.LZMA2, Compressor.GZIP, Compressor.GZIP, Compressor.GZIP,  Compressor.GZIP,
                      Compressor.UNCOMPRESSED, Compressor.SNAPPY, Compressor.LZ4, Compressor.GZIP, Compressor.ZSTD, Compressor.LZMA2, Compressor.GZIP, Compressor.GZIP, Compressor.GZIP,  Compressor.GZIP]
    session.create_multi_time_series(
        ts_path_lst, data_type_lst, encoding_lst, compressor_lst,
        props_lst=None, tags_lst=None, attributes_lst=None, alias_lst=None
    )

def create_aligned_timeseries(session):   
    device_id = "root.test.g3.d1"
    measurements_lst = ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB", "STRING",]
    data_type_lst = [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE, TSDataType.TEXT, TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB, TSDataType.STRING,]
    encoding_lst = [TSEncoding.RLE, TSEncoding.CHIMP, TSEncoding.ZIGZAG, TSEncoding.RLBE, TSEncoding.SPRINTZ, TSEncoding.DICTIONARY, TSEncoding.TS_2DIFF, TSEncoding.CHIMP, TSEncoding.PLAIN, TSEncoding.DICTIONARY,]
    compressor_lst = [Compressor.UNCOMPRESSED, Compressor.SNAPPY, Compressor.LZ4, Compressor.GZIP, Compressor.ZSTD, Compressor.LZMA2, Compressor.GZIP, Compressor.GZIP, Compressor.GZIP,  Compressor.GZIP,]
    session.create_aligned_time_series(device_id, measurements_lst, data_type_lst, encoding_lst, compressor_lst)

    device_id = "root.test.g3.d2"
    measurements_lst = ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB", "STRING",]
    data_type_lst = [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE, TSDataType.TEXT, TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB, TSDataType.STRING,]
    encoding_lst = [TSEncoding.RLE, TSEncoding.CHIMP, TSEncoding.ZIGZAG, TSEncoding.RLBE, TSEncoding.SPRINTZ, TSEncoding.DICTIONARY, TSEncoding.TS_2DIFF, TSEncoding.CHIMP, TSEncoding.PLAIN, TSEncoding.DICTIONARY,]
    compressor_lst = [Compressor.UNCOMPRESSED, Compressor.SNAPPY, Compressor.LZ4, Compressor.GZIP, Compressor.ZSTD, Compressor.LZMA2, Compressor.GZIP, Compressor.GZIP, Compressor.GZIP,  Compressor.GZIP,]
    session.create_aligned_time_series(device_id, measurements_lst, data_type_lst, encoding_lst, compressor_lst)

    device_id = "root.test.g3.d3"
    measurements_lst = ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB", "STRING",]
    data_type_lst = [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE, TSDataType.TEXT, TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB, TSDataType.STRING,]
    encoding_lst = [TSEncoding.RLE, TSEncoding.CHIMP, TSEncoding.ZIGZAG, TSEncoding.RLBE, TSEncoding.SPRINTZ, TSEncoding.DICTIONARY, TSEncoding.TS_2DIFF, TSEncoding.CHIMP, TSEncoding.PLAIN, TSEncoding.DICTIONARY,]
    compressor_lst = [Compressor.UNCOMPRESSED, Compressor.SNAPPY, Compressor.LZ4, Compressor.GZIP, Compressor.ZSTD, Compressor.LZMA2, Compressor.GZIP, Compressor.GZIP, Compressor.GZIP,  Compressor.GZIP,]
    session.create_aligned_time_series(device_id, measurements_lst, data_type_lst, encoding_lst, compressor_lst)

    device_id = "root.test.g4.d1"
    measurements_lst = ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB", "STRING",]
    data_type_lst = [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE, TSDataType.TEXT, TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB, TSDataType.STRING,]
    encoding_lst = [TSEncoding.RLE, TSEncoding.CHIMP, TSEncoding.ZIGZAG, TSEncoding.RLBE, TSEncoding.SPRINTZ, TSEncoding.DICTIONARY, TSEncoding.TS_2DIFF, TSEncoding.CHIMP, TSEncoding.PLAIN, TSEncoding.DICTIONARY,]
    compressor_lst = [Compressor.UNCOMPRESSED, Compressor.SNAPPY, Compressor.LZ4, Compressor.GZIP, Compressor.ZSTD, Compressor.LZMA2, Compressor.GZIP, Compressor.GZIP, Compressor.GZIP,  Compressor.GZIP,]
    session.create_aligned_time_series(device_id, measurements_lst, data_type_lst, encoding_lst, compressor_lst)

def delete_database(session):
    group_name_lst = ["root.test.g1", "root.test.g2", "root.test.g3", "root.test.g4", "root.test.g5", "root.test.g6"]
    session.delete_storage_groups(group_name_lst)

def query(sql):
    actual = 0
    with session.execute_query_statement(
        sql
    ) as session_data_set:
        session_data_set.set_fetch_size(1024)
        while session_data_set.has_next():
            print(session_data_set.next())
            actual = actual + 1
            
    return actual

# Environment setup and clearing before and after each test case
@pytest.fixture()
def fixture_():
    session = get_session()
    create_database(session)
    create_timeseries(session)
    create_aligned_timeseries(session)
    
    yield
    try:
        delete_database(session)
    except Exception as e:
        print(e)

    session.close()

# Test writing a tablet data to a non aligned time series
@pytest.mark.usefixtures('fixture_')
def test_insert_tablet():
    global session
    # 1、Inserting tablet data
    expect = 10
    device_id = "root.test.g1.d1"
    measurements_ = ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB", "STRING"]
    data_types_ = [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE, TSDataType.TEXT, TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB, TSDataType.STRING]
    values_ = [
        [False, 0, 0, 0.0, 0.0, "1234567890", 0, date(1970, 1, 1), '1234567890'.encode('utf-8'), "1234567890"],
        [True, -2147483648, -9223372036854775808, -0.12345678, -0.12345678901234567, "abcdefghijklmnopqrstuvwsyz", -9223372036854775808, date(1000, 1, 1), 'abcdefghijklmnopqrstuvwsyz'.encode('utf-8'), "abcdefghijklmnopqrstuvwsyz"],
        [True, 2147483647, 9223372036854775807, 0.123456789, 0.12345678901234567, "!@#$%^&*()_+}{|:'`~-=[];,./<>?~", 9223372036854775807, date(9999, 12, 31), '!@#$%^&*()_+}{|:`~-=[];,./<>?~'.encode('utf-8'), "!@#$%^&*()_+}{|:`~-=[];,./<>?~"],
        [True, 1, 1, 1.0, 1.0, "没问题", 1, date(1970, 1, 1), '没问题'.encode('utf-8'), "没问题"],
        [True, -1, -1, 1.1234567, 1.1234567890123456, "！@#￥%……&*（）——|：“《》？·【】、；‘，。/", 11, date(1970, 1, 1), '！@#￥%……&*（）——|：“《》？·【】、；‘，。/'.encode('utf-8'), "！@#￥%……&*（）——|：“《》？·【】、；‘，。/"],
        [True, 10, 11, 4.123456, 4.123456789012345, "1234567890abcdefghijklmnopqrstuvwsyz!@#$%^&*()_+}{|:'`~-=[];,./<>?~！@#￥%……&*（）——|：“《》？·【】、；‘，。/没问题", 11, date(1970, 1, 1), '1234567890abcdefghijklmnopqrstuvwsyz!@#$%^&*()_+}{|:`~-=[];,./<>?~！@#￥%……&*（）——|：“《》？·【】、；‘，。/没问题'.encode('utf-8'), "1234567890abcdefghijklmnopqrstuvwsyz!@#$%^&*()_+}{|:`~-=[];,./<>?~！@#￥%……&*（）——|：“《》？·【】、；‘，。/没问题"],
        [True, -10, -11, 12.12345, 12.12345678901234, "test01", 11, date(1970, 1, 1), 'Hello, World!'.encode('utf-8'), "string01"],
        [None, None, None, None, None, "", None, date(1970, 1, 1), ''.encode('utf-8'), ""],
        [True, -0, -0, -0.0, -0.0, "    ", 11, date(1970, 1, 1), '    '.encode('utf-8'), "    "],
        [True, 10, 11, 1.1, 10011.1, "test01", 11, date(1970, 1, 1), 'Hello, World!'.encode('utf-8'), "string01"]
    ]
    timestamps_ = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
    tablet_ = Tablet(device_id, measurements_, data_types_, values_, timestamps_)
    session.insert_tablet(tablet_)
    # Calculate the number of rows in the actual time series
    actual = query("select * from root.test.g1.d1")
    # Determine whether it meets expectations
    assert expect == actual

    # 2、Inserting Numpy Tablet data (No null)
    expect = 10
    device_id = "root.test.g1.d2"
    measurements_ = ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB", "STRING"]
    data_types_ = [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE, TSDataType.TEXT, TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB, TSDataType.STRING]
    np_values_ = [
        np.array([False, True, False, True, False, True, False, True, False, True], TSDataType.BOOLEAN.np_dtype()),
        np.array([0, -2147483648, 2147483647, 1, -1, 10, -10, -0, 10, 10], TSDataType.INT32.np_dtype()),
        np.array([0, -9223372036854775808, 9223372036854775807, 1, -1, 10, -10, -0, 10, 10], TSDataType.INT64.np_dtype()),
        np.array([0.0, -0.12345678, 0.123456789, 1.0, 1.1234567, 4.123456, 12.12345, -0.0, 1.1, 1.1], TSDataType.FLOAT.np_dtype()),
        np.array([0.0, -0.12345678901234567, 0.12345678901234567, 1.0, 1.1234567890123456, 4.123456789012345, 12.12345678901234, -0.0, 1.1, 1.1], TSDataType.DOUBLE.np_dtype()),
        np.array(["1234567890", "abcdefghijklmnopqrstuvwsyz", "!@#$%^&*()_+}{|:'`~-=[];,./<>?~", "没问题", "！@#￥%……&*（）——|：“《》？·【】、；‘，。/", "1234567890abcdefghijklmnopqrstuvwsyz!@#$%^&*()_+}{|:'`~-=[];,./<>?~！@#￥%……&*（）——|：“《》？·【】、；‘，。/没问题", "", "    ", "test01", "test01"], TSDataType.TEXT.np_dtype()),
        np.array([0, -9223372036854775808, 9223372036854775807, 1, -1, 10, -10, -0, 10, 10], TSDataType.TIMESTAMP.np_dtype()),
        np.array([date(1970, 1, 1), date(1000, 1, 1), date(9999, 12, 31), date(1970, 1, 1), date(1970, 1, 1), date(1970, 1, 1), date(1970, 1, 1), date(1970, 1, 1), date(1970, 1, 1), date(1970, 1, 1)], TSDataType.DATE.np_dtype()),
        np.array(['1234567890'.encode('utf-8'), 'abcdefghijklmnopqrstuvwsyz'.encode('utf-8'), '!@#$%^&*()_+}{|:`~-=[];,./<>?~'.encode('utf-8'), '没问题'.encode('utf-8'), '！@#￥%……&*（）——|：“《》？·【】、；‘，。/'.encode('utf-8'), '1234567890abcdefghijklmnopqrstuvwsyz!@#$%^&*()_+}{|:`~-=[];,./<>?~！@#￥%……&*（）——|：“《》？·【】、；‘，。/没问题'.encode('utf-8'), ''.encode('utf-8'), '   '.encode('utf-8'), '1234567890'.encode('utf-8'), '1234567890'.encode('utf-8')], TSDataType.BLOB.np_dtype()),
        np.array(["1234567890", "abcdefghijklmnopqrstuvwsyz", "!@#$%^&*()_+}{|:'`~-=[];,./<>?~", "没问题", "！@#￥%……&*（）——|：“《》？·【】、；‘，。/", "1234567890abcdefghijklmnopqrstuvwsyz!@#$%^&*()_+}{|:'`~-=[];,./<>?~！@#￥%……&*（）——|：“《》？·【】、；‘，。/没问题", "", "    ", "test01", "test01"], TSDataType.STRING.np_dtype())
    ]
    np_timestamps_ = np.array([1, 2, 3, 4, 5, 6, 7, 8, 9, 10], TSDataType.INT64.np_dtype())
    np_tablet_ = NumpyTablet(device_id, measurements_, data_types_, np_values_, np_timestamps_)
    session.insert_tablet(np_tablet_)
    # Calculate the number of rows in the actual time series
    actual = query("select * from root.test.g1.d2")
    # Determine whether it meets expectations
    assert expect == actual

    # 3、Inserting Numpy Tablet data (Contains null)
    expect = 10
    device_id = "root.test.g1.d3"
    measurements_ = ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB", "STRING"]
    data_types_ = [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE, TSDataType.TEXT, TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB, TSDataType.STRING]
    np_values_ = [
        np.array([False, True, False, True, False, True, False, True, False, False], TSDataType.BOOLEAN.np_dtype()),
        np.array([0, -2147483648, 2147483647, 1, -1, 10, -10, -0, 10, 10], TSDataType.INT32.np_dtype()),
        np.array([0, -9223372036854775808, 9223372036854775807, 1, -1, 10, -10, -0, 10, 10], TSDataType.INT64.np_dtype()),
        np.array([0.0, -0.12345678, 0.123456789, 1.0, 1.1234567, 4.123456, 12.12345, -0.0, 1.1, 1.1], TSDataType.FLOAT.np_dtype()),
        np.array([0.0, -0.12345678901234567, 0.12345678901234567, 1.0, 1.1234567890123456, 4.123456789012345, 12.12345678901234, -0.0, 1.1, 1.1], TSDataType.DOUBLE.np_dtype()),
        np.array(["1234567890", "abcdefghijklmnopqrstuvwsyz", "!@#$%^&*()_+}{|:'`~-=[];,./<>?~", "没问题", "！@#￥%……&*（）——|：“《》？·【】、；‘，。/", "1234567890abcdefghijklmnopqrstuvwsyz!@#$%^&*()_+}{|:'`~-=[];,./<>?~！@#￥%……&*（）——|：“《》？·【】、；‘，。/没问题", "", "    ", "test01", "test01"], TSDataType.TEXT.np_dtype()),
        np.array([0, -9223372036854775808, 9223372036854775807, 1, -1, 10, -10, -0, 10, 10], TSDataType.TIMESTAMP.np_dtype()),
        np.array([date(1970, 1, 1), date(1000, 1, 1), date(9999, 12, 31), date(1970, 1, 1), date(1970, 1, 1), date(1970, 1, 1), date(1970, 1, 1), date(1970, 1, 1), date(1970, 1, 1), date(1970, 1, 1)], TSDataType.DATE.np_dtype()),
        np.array(['1234567890'.encode('utf-8'), 'abcdefghijklmnopqrstuvwsyz'.encode('utf-8'), '!@#$%^&*()_+}{|:`~-=[];,./<>?~'.encode('utf-8'), '没问题'.encode('utf-8'), '！@#￥%……&*（）——|：“《》？·【】、；‘，。/'.encode('utf-8'), '1234567890abcdefghijklmnopqrstuvwsyz!@#$%^&*()_+}{|:`~-=[];,./<>?~！@#￥%……&*（）——|：“《》？·【】、；‘，。/没问题'.encode('utf-8'), ''.encode('utf-8'), '   '.encode('utf-8'), '1234567890'.encode('utf-8'), '1234567890'.encode('utf-8')], TSDataType.BLOB.np_dtype()),
        np.array(["1234567890", "abcdefghijklmnopqrstuvwsyz", "!@#$%^&*()_+}{|:'`~-=[];,./<>?~", "没问题", "！@#￥%……&*（）——|：“《》？·【】、；‘，。/", "1234567890abcdefghijklmnopqrstuvwsyz!@#$%^&*()_+}{|:'`~-=[];,./<>?~！@#￥%……&*（）——|：“《》？·【】、；‘，。/没问题", "", "    ", "test01", "test01"], TSDataType.STRING.np_dtype())
    ]
    np_timestamps_ = np.array([1, 2, 3, 4, 5, 6, 7, 8, 9, 10], TSDataType.INT64.np_dtype())
    np_bitmaps_ = []
    for i in range(len(measurements_)):
        np_bitmaps_.append(BitMap(len(np_timestamps_)))
    np_bitmaps_[0].mark(9)
    np_bitmaps_[1].mark(8)
    np_bitmaps_[2].mark(9)
    np_bitmaps_[4].mark(8)
    np_bitmaps_[5].mark(9)
    np_bitmaps_[6].mark(8)
    np_bitmaps_[7].mark(9)
    np_bitmaps_[8].mark(8)
    np_bitmaps_[9].mark(9)
    np_tablet_ = NumpyTablet(device_id, measurements_, data_types_, np_values_, np_timestamps_, np_bitmaps_)
    session.insert_tablet(np_tablet_)
    # Calculate the number of rows in the actual time series
    actual = query("select * from root.test.g1.d3")
    # Determine whether it meets expectations
    assert expect == actual

# Test writing a tablet data to align the time series
@pytest.mark.usefixtures('fixture_')
def test_insert_aligned_tablet():
    global session
    # 1、Inserting tablet data
    expect = 10
    device_id = "root.test.g3.d1"
    measurements_ = ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB", "STRING"]
    data_types_ = [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE, TSDataType.TEXT, TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB, TSDataType.STRING]
    values_ = [
            [False, 0, 0, 0.0, 0.0, "1234567890", 0, date(1970, 1, 1), '1234567890'.encode('utf-8'), "1234567890"],
            [True, -2147483648, -9223372036854775808, -0.12345678, -0.12345678901234567, "abcdefghijklmnopqrstuvwsyz", -9223372036854775808, date(1000, 1, 1), 'abcdefghijklmnopqrstuvwsyz'.encode('utf-8'), "abcdefghijklmnopqrstuvwsyz"],
            [True, 2147483647, 9223372036854775807, 0.123456789, 0.12345678901234567, "!@#$%^&*()_+}{|:'`~-=[];,./<>?~", 9223372036854775807, date(9999, 12, 31), '!@#$%^&*()_+}{|:`~-=[];,./<>?~'.encode('utf-8'), "!@#$%^&*()_+}{|:`~-=[];,./<>?~"],
            [True, 1, 1, 1.0, 1.0, "没问题", 1, date(1970, 1, 1), '没问题'.encode('utf-8'), "没问题"],
            [True, -1, -1, 1.1234567, 1.1234567890123456, "！@#￥%……&*（）——|：“《》？·【】、；‘，。/", 11, date(1970, 1, 1), '！@#￥%……&*（）——|：“《》？·【】、；‘，。/'.encode('utf-8'), "！@#￥%……&*（）——|：“《》？·【】、；‘，。/"],
            [True, 10, 11, 4.123456, 4.123456789012345, "1234567890abcdefghijklmnopqrstuvwsyz!@#$%^&*()_+}{|:'`~-=[];,./<>?~！@#￥%……&*（）——|：“《》？·【】、；‘，。/没问题", 11, date(1970, 1, 1), '1234567890abcdefghijklmnopqrstuvwsyz!@#$%^&*()_+}{|:`~-=[];,./<>?~！@#￥%……&*（）——|：“《》？·【】、；‘，。/没问题'.encode('utf-8'), "1234567890abcdefghijklmnopqrstuvwsyz!@#$%^&*()_+}{|:`~-=[];,./<>?~！@#￥%……&*（）——|：“《》？·【】、；‘，。/没问题"],
            [True, -10, -11, 12.12345, 12.12345678901234, "test01", 11, date(1970, 1, 1), 'Hello, World!'.encode('utf-8'), "string01"],
            [None, None, None, None, None, "", None, date(1970, 1, 1), ''.encode('utf-8'), ""],
            [True, -0, -0, -0.0, -0.0, "    ", 11, date(1970, 1, 1), '    '.encode('utf-8'), "    "],
            [True, 10, 11, 1.1, 10011.1, "test01", 11, date(1970, 1, 1), 'Hello, World!'.encode('utf-8'), "string01"]
    ]
    timestamps_ = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
    tablet_ = Tablet(device_id, measurements_, data_types_, values_, timestamps_)
    session.insert_aligned_tablet(tablet_)
    # Calculate the number of rows in the actual time series
    actual = query("select * from root.test.g3.d1")
    # Determine whether it meets expectations
    assert expect == actual

    # 2、Inserting Numpy Tablet data (No null)
    expect = 10
    device_id = "root.test.g3.d2"
    measurements_ = ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB", "STRING"]
    data_types_ = [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE, TSDataType.TEXT, TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB, TSDataType.STRING]
    np_values_ = [
        np.array([False, True, False, True, False, True, False, True, False, True], TSDataType.BOOLEAN.np_dtype()),
        np.array([0, -2147483648, 2147483647, 1, -1, 10, -10, -0, 10, 10], TSDataType.INT32.np_dtype()),
        np.array([0, -9223372036854775808, 9223372036854775807, 1, -1, 10, -10, -0, 10, 10], TSDataType.INT64.np_dtype()),
        np.array([0.0, -0.12345678, 0.123456789, 1.0, 1.1234567, 4.123456, 12.12345, -0.0, 1.1, 1.1], TSDataType.FLOAT.np_dtype()),
        np.array([0.0, -0.12345678901234567, 0.12345678901234567, 1.0, 1.1234567890123456, 4.123456789012345, 12.12345678901234, -0.0, 1.1, 1.1], TSDataType.DOUBLE.np_dtype()),
        np.array(["1234567890", "abcdefghijklmnopqrstuvwsyz", "!@#$%^&*()_+}{|:'`~-=[];,./<>?~", "没问题", "！@#￥%……&*（）——|：“《》？·【】、；‘，。/", "1234567890abcdefghijklmnopqrstuvwsyz!@#$%^&*()_+}{|:'`~-=[];,./<>?~！@#￥%……&*（）——|：“《》？·【】、；‘，。/没问题", "", "    ", "test01", "test01"], TSDataType.TEXT.np_dtype()),
        np.array([0, -9223372036854775808, 9223372036854775807, 1, -1, 10, -10, -0, 10, 10], TSDataType.TIMESTAMP.np_dtype()),
        np.array([date(1970, 1, 1), date(1000, 1, 1), date(9999, 12, 31), date(1970, 1, 1), date(1970, 1, 1), date(1970, 1, 1), date(1970, 1, 1), date(1970, 1, 1), date(1970, 1, 1), date(1970, 1, 1)], TSDataType.DATE.np_dtype()),
        np.array(['1234567890'.encode('utf-8'), 'abcdefghijklmnopqrstuvwsyz'.encode('utf-8'), '!@#$%^&*()_+}{|:`~-=[];,./<>?~'.encode('utf-8'), '没问题'.encode('utf-8'), '！@#￥%……&*（）——|：“《》？·【】、；‘，。/'.encode('utf-8'), '1234567890abcdefghijklmnopqrstuvwsyz!@#$%^&*()_+}{|:`~-=[];,./<>?~！@#￥%……&*（）——|：“《》？·【】、；‘，。/没问题'.encode('utf-8'), ''.encode('utf-8'), '   '.encode('utf-8'), '1234567890'.encode('utf-8'), '1234567890'.encode('utf-8')], TSDataType.BLOB.np_dtype()),
        np.array(["1234567890", "abcdefghijklmnopqrstuvwsyz", "!@#$%^&*()_+}{|:'`~-=[];,./<>?~", "没问题", "！@#￥%……&*（）——|：“《》？·【】、；‘，。/", "1234567890abcdefghijklmnopqrstuvwsyz!@#$%^&*()_+}{|:'`~-=[];,./<>?~！@#￥%……&*（）——|：“《》？·【】、；‘，。/没问题", "", "    ", "test01", "test01"], TSDataType.STRING.np_dtype())
    ]
    np_timestamps_ = np.array([1, 2, 3, 4, 5, 6, 7, 8, 9, 10], TSDataType.INT64.np_dtype())
    np_tablet_ = NumpyTablet(device_id, measurements_, data_types_, np_values_, np_timestamps_)
    session.insert_aligned_tablet(np_tablet_)
    # Calculate the number of rows in the actual time series
    actual = query("select * from root.test.g3.d2")
    # Determine whether it meets expectations
    assert expect == actual

    # 3、Inserting Numpy Tablet data (Contains null)
    expect = 10
    device_id = "root.test.g3.d3"
    measurements_ = ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB", "STRING"]
    data_types_ = [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE, TSDataType.TEXT, TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB, TSDataType.STRING]
    np_values_ = [
        np.array([False, True, False, True, False, True, False, True, False, False], TSDataType.BOOLEAN.np_dtype()),
        np.array([0, -2147483648, 2147483647, 1, -1, 10, -10, -0, 10, 10], TSDataType.INT32.np_dtype()),
        np.array([0, -9223372036854775808, 9223372036854775807, 1, -1, 10, -10, -0, 10, 10], TSDataType.INT64.np_dtype()),
        np.array([0.0, -0.12345678, 0.123456789, 1.0, 1.1234567, 4.123456, 12.12345, -0.0, 1.1, 1.1], TSDataType.FLOAT.np_dtype()),
        np.array([0.0, -0.12345678901234567, 0.12345678901234567, 1.0, 1.1234567890123456, 4.123456789012345, 12.12345678901234, -0.0, 1.1, 1.1], TSDataType.DOUBLE.np_dtype()),
        np.array(["1234567890", "abcdefghijklmnopqrstuvwsyz", "!@#$%^&*()_+}{|:'`~-=[];,./<>?~", "没问题", "！@#￥%……&*（）——|：“《》？·【】、；‘，。/", "1234567890abcdefghijklmnopqrstuvwsyz!@#$%^&*()_+}{|:'`~-=[];,./<>?~！@#￥%……&*（）——|：“《》？·【】、；‘，。/没问题", "", "    ", "test01", "test01"], TSDataType.TEXT.np_dtype()),
        np.array([0, -9223372036854775808, 9223372036854775807, 1, -1, 10, -10, -0, 10, 10], TSDataType.TIMESTAMP.np_dtype()),
        np.array([date(1970, 1, 1), date(1000, 1, 1), date(9999, 12, 31), date(1970, 1, 1), date(1970, 1, 1), date(1970, 1, 1), date(1970, 1, 1), date(1970, 1, 1), date(1970, 1, 1), date(1970, 1, 1)], TSDataType.DATE.np_dtype()),
        np.array(['1234567890'.encode('utf-8'), 'abcdefghijklmnopqrstuvwsyz'.encode('utf-8'), '!@#$%^&*()_+}{|:`~-=[];,./<>?~'.encode('utf-8'), '没问题'.encode('utf-8'), '！@#￥%……&*（）——|：“《》？·【】、；‘，。/'.encode('utf-8'), '1234567890abcdefghijklmnopqrstuvwsyz!@#$%^&*()_+}{|:`~-=[];,./<>?~！@#￥%……&*（）——|：“《》？·【】、；‘，。/没问题'.encode('utf-8'), ''.encode('utf-8'), '   '.encode('utf-8'), '1234567890'.encode('utf-8'), '1234567890'.encode('utf-8')], TSDataType.BLOB.np_dtype()),
        np.array(["1234567890", "abcdefghijklmnopqrstuvwsyz", "!@#$%^&*()_+}{|:'`~-=[];,./<>?~", "没问题", "！@#￥%……&*（）——|：“《》？·【】、；‘，。/", "1234567890abcdefghijklmnopqrstuvwsyz!@#$%^&*()_+}{|:'`~-=[];,./<>?~！@#￥%……&*（）——|：“《》？·【】、；‘，。/没问题", "", "    ", "test01", "test01"], TSDataType.STRING.np_dtype())
    ]
    np_timestamps_ = np.array([1, 2, 3, 4, 5, 6, 7, 8, 9, 10], TSDataType.INT64.np_dtype())
    np_bitmaps_ = []
    for i in range(len(measurements_)):
        np_bitmaps_.append(BitMap(len(np_timestamps_)))
    np_bitmaps_[0].mark(9)
    np_bitmaps_[1].mark(8)
    np_bitmaps_[2].mark(9)
    np_bitmaps_[4].mark(8)
    np_bitmaps_[5].mark(9)
    np_bitmaps_[6].mark(8)
    np_bitmaps_[7].mark(9)
    np_bitmaps_[8].mark(8)
    np_bitmaps_[9].mark(9)
    np_tablet_ = NumpyTablet(device_id, measurements_, data_types_, np_values_, np_timestamps_, np_bitmaps_)
    session.insert_aligned_tablet(np_tablet_)
    # Calculate the number of rows in the actual time series
    actual = query("select * from root.test.g3.d3")
    # Determine whether it meets expectations
    assert expect == actual

# Test writing multiple tablet data to non aligned time series
@pytest.mark.usefixtures('fixture_')
def test_insert_tablets():
    global session
    # 1、Inserting tablet data
    expect = 40
    device1_id = "root.test.g1.d1"
    measurements_ = ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB", "STRING"]
    data_types = [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE, TSDataType.TEXT, TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB, TSDataType.STRING]
    values_ = [
        [False, 0, 0, 0.0, 0.0, "1234567890", 0, date(1970, 1, 1), '1234567890'.encode('utf-8'), "1234567890"],
        [True, -2147483648, -9223372036854775808, -0.12345678, -0.12345678901234567, "abcdefghijklmnopqrstuvwsyz", -9223372036854775808, date(1000, 1, 1), 'abcdefghijklmnopqrstuvwsyz'.encode('utf-8'), "abcdefghijklmnopqrstuvwsyz"],
        [True, 2147483647, 9223372036854775807, 0.123456789, 0.12345678901234567, "!@#$%^&*()_+}{|:'`~-=[];,./<>?~", 9223372036854775807, date(9999, 12, 31), '!@#$%^&*()_+}{|:`~-=[];,./<>?~'.encode('utf-8'), "!@#$%^&*()_+}{|:`~-=[];,./<>?~"],
        [True, 1, 1, 1.0, 1.0, "没问题", 1, date(1970, 1, 1), '没问题'.encode('utf-8'), "没问题"],
        [True, -1, -1, 1.1234567, 1.1234567890123456, "！@#￥%……&*（）——|：“《》？·【】、；‘，。/", 11, date(1970, 1, 1), '！@#￥%……&*（）——|：“《》？·【】、；‘，。/'.encode('utf-8'), "！@#￥%……&*（）——|：“《》？·【】、；‘，。/"],
        [True, 10, 11, 4.123456, 4.123456789012345, "1234567890abcdefghijklmnopqrstuvwsyz!@#$%^&*()_+}{|:'`~-=[];,./<>?~！@#￥%……&*（）——|：“《》？·【】、；‘，。/没问题", 11, date(1970, 1, 1), '1234567890abcdefghijklmnopqrstuvwsyz!@#$%^&*()_+}{|:`~-=[];,./<>?~！@#￥%……&*（）——|：“《》？·【】、；‘，。/没问题'.encode('utf-8'), "1234567890abcdefghijklmnopqrstuvwsyz!@#$%^&*()_+}{|:`~-=[];,./<>?~！@#￥%……&*（）——|：“《》？·【】、；‘，。/没问题"],
        [True, -10, -11, 12.12345, 12.12345678901234, "test01", 11, date(1970, 1, 1), 'Hello, World!'.encode('utf-8'), "string01"],
        [None, None, None, None, None, "", None, date(1970, 1, 1), ''.encode('utf-8'), ""],
        [True, -0, -0, -0.0, -0.0, "    ", 11, date(1970, 1, 1), '    '.encode('utf-8'), "    "],
        [True, 10, 11, 1.1, 10011.1, "test01", 11, date(1970, 1, 1), 'Hello, World!'.encode('utf-8'), "string01"]
    ]
    timestamps_ = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
    tablet1_ = Tablet(device1_id, measurements_, data_types, values_, timestamps_)
    device1_id = "root.test.g1.d1"
    measurements_ = ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB", "STRING"]
    data_types = [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE, TSDataType.TEXT, TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB, TSDataType.STRING]
    values_ = [
        [False, 0, 0, 0.0, 0.0, "1234567890", 0, date(1970, 1, 1), '1234567890'.encode('utf-8'), "1234567890"],
        [True, -2147483648, -9223372036854775808, -0.12345678, -0.12345678901234567, "abcdefghijklmnopqrstuvwsyz", -9223372036854775808, date(1000, 1, 1), 'abcdefghijklmnopqrstuvwsyz'.encode('utf-8'), "abcdefghijklmnopqrstuvwsyz"],
        [True, 2147483647, 9223372036854775807, 0.123456789, 0.12345678901234567, "!@#$%^&*()_+}{|:'`~-=[];,./<>?~", 9223372036854775807, date(9999, 12, 31), '!@#$%^&*()_+}{|:`~-=[];,./<>?~'.encode('utf-8'), "!@#$%^&*()_+}{|:`~-=[];,./<>?~"],
        [True, 1, 1, 1.0, 1.0, "没问题", 1, date(1970, 1, 1), '没问题'.encode('utf-8'), "没问题"],
        [True, -1, -1, 1.1234567, 1.1234567890123456, "！@#￥%……&*（）——|：“《》？·【】、；‘，。/", 11, date(1970, 1, 1), '！@#￥%……&*（）——|：“《》？·【】、；‘，。/'.encode('utf-8'), "！@#￥%……&*（）——|：“《》？·【】、；‘，。/"],
        [True, 10, 11, 4.123456, 4.123456789012345, "1234567890abcdefghijklmnopqrstuvwsyz!@#$%^&*()_+}{|:'`~-=[];,./<>?~！@#￥%……&*（）——|：“《》？·【】、；‘，。/没问题", 11, date(1970, 1, 1), '1234567890abcdefghijklmnopqrstuvwsyz!@#$%^&*()_+}{|:`~-=[];,./<>?~！@#￥%……&*（）——|：“《》？·【】、；‘，。/没问题'.encode('utf-8'), "1234567890abcdefghijklmnopqrstuvwsyz!@#$%^&*()_+}{|:`~-=[];,./<>?~！@#￥%……&*（）——|：“《》？·【】、；‘，。/没问题"],
        [True, -10, -11, 12.12345, 12.12345678901234, "test01", 11, date(1970, 1, 1), 'Hello, World!'.encode('utf-8'), "string01"],
        [None, None, None, None, None, "", None, date(1970, 1, 1), ''.encode('utf-8'), ""],
        [True, -0, -0, -0.0, -0.0, "    ", 11, date(1970, 1, 1), '    '.encode('utf-8'), "    "],
        [True, 10, 11, 1.1, 10011.1, "test01", 11, date(1970, 1, 1), 'Hello, World!'.encode('utf-8'), "string01"]
    ]
    timestamps_ = [11, 12, 13, 14, 15, 16, 17, 18, 19, 20]
    tablet2_ = Tablet(device1_id, measurements_, data_types, values_, timestamps_)

    device2_id = "root.test.g1.d2"
    measurements_ = ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB", "STRING"]
    data_types = [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE, TSDataType.TEXT, TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB, TSDataType.STRING]
    values_ = [
        [False, 0, 0, 0.0, 0.0, "1234567890", 0, date(1970, 1, 1), '1234567890'.encode('utf-8'), "1234567890"],
        [True, -2147483648, -9223372036854775808, -0.12345678, -0.12345678901234567, "abcdefghijklmnopqrstuvwsyz", -9223372036854775808, date(1000, 1, 1), 'abcdefghijklmnopqrstuvwsyz'.encode('utf-8'), "abcdefghijklmnopqrstuvwsyz"],
        [True, 2147483647, 9223372036854775807, 0.123456789, 0.12345678901234567, "!@#$%^&*()_+}{|:'`~-=[];,./<>?~", 9223372036854775807, date(9999, 12, 31), '!@#$%^&*()_+}{|:`~-=[];,./<>?~'.encode('utf-8'), "!@#$%^&*()_+}{|:`~-=[];,./<>?~"],
        [True, 1, 1, 1.0, 1.0, "没问题", 1, date(1970, 1, 1), '没问题'.encode('utf-8'), "没问题"],
        [True, -1, -1, 1.1234567, 1.1234567890123456, "！@#￥%……&*（）——|：“《》？·【】、；‘，。/", 11, date(1970, 1, 1), '！@#￥%……&*（）——|：“《》？·【】、；‘，。/'.encode('utf-8'), "！@#￥%……&*（）——|：“《》？·【】、；‘，。/"],
        [True, 10, 11, 4.123456, 4.123456789012345, "1234567890abcdefghijklmnopqrstuvwsyz!@#$%^&*()_+}{|:'`~-=[];,./<>?~！@#￥%……&*（）——|：“《》？·【】、；‘，。/没问题", 11, date(1970, 1, 1), '1234567890abcdefghijklmnopqrstuvwsyz!@#$%^&*()_+}{|:`~-=[];,./<>?~！@#￥%……&*（）——|：“《》？·【】、；‘，。/没问题'.encode('utf-8'), "1234567890abcdefghijklmnopqrstuvwsyz!@#$%^&*()_+}{|:`~-=[];,./<>?~！@#￥%……&*（）——|：“《》？·【】、；‘，。/没问题"],
        [True, -10, -11, 12.12345, 12.12345678901234, "test01", 11, date(1970, 1, 1), 'Hello, World!'.encode('utf-8'), "string01"],
        [None, None, None, None, None, "", None, date(1970, 1, 1), ''.encode('utf-8'), ""],
        [True, -0, -0, -0.0, -0.0, "    ", 11, date(1970, 1, 1), '    '.encode('utf-8'), "    "],
        [True, 10, 11, 1.1, 10011.1, "test01", 11, date(1970, 1, 1), 'Hello, World!'.encode('utf-8'), "string01"]
    ]
    timestamps_ = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
    tablet3_ = Tablet(device2_id, measurements_, data_types, values_, timestamps_)

    device2_id = "root.test.g2.d1"
    measurements_ = ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB", "STRING"]
    data_types = [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE, TSDataType.TEXT, TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB, TSDataType.STRING]
    values_ = [
        [False, 0, 0, 0.0, 0.0, "1234567890", 0, date(1970, 1, 1), '1234567890'.encode('utf-8'), "1234567890"],
        [True, -2147483648, -9223372036854775808, -0.12345678, -0.12345678901234567, "abcdefghijklmnopqrstuvwsyz", -9223372036854775808, date(1000, 1, 1), 'abcdefghijklmnopqrstuvwsyz'.encode('utf-8'), "abcdefghijklmnopqrstuvwsyz"],
        [True, 2147483647, 9223372036854775807, 0.123456789, 0.12345678901234567, "!@#$%^&*()_+}{|:'`~-=[];,./<>?~", 9223372036854775807, date(9999, 12, 31), '!@#$%^&*()_+}{|:`~-=[];,./<>?~'.encode('utf-8'), "!@#$%^&*()_+}{|:`~-=[];,./<>?~"],
        [True, 1, 1, 1.0, 1.0, "没问题", 1, date(1970, 1, 1), '没问题'.encode('utf-8'), "没问题"],
        [True, -1, -1, 1.1234567, 1.1234567890123456, "！@#￥%……&*（）——|：“《》？·【】、；‘，。/", 11, date(1970, 1, 1), '！@#￥%……&*（）——|：“《》？·【】、；‘，。/'.encode('utf-8'), "！@#￥%……&*（）——|：“《》？·【】、；‘，。/"],
        [True, 10, 11, 4.123456, 4.123456789012345, "1234567890abcdefghijklmnopqrstuvwsyz!@#$%^&*()_+}{|:'`~-=[];,./<>?~！@#￥%……&*（）——|：“《》？·【】、；‘，。/没问题", 11, date(1970, 1, 1), '1234567890abcdefghijklmnopqrstuvwsyz!@#$%^&*()_+}{|:`~-=[];,./<>?~！@#￥%……&*（）——|：“《》？·【】、；‘，。/没问题'.encode('utf-8'), "1234567890abcdefghijklmnopqrstuvwsyz!@#$%^&*()_+}{|:`~-=[];,./<>?~！@#￥%……&*（）——|：“《》？·【】、；‘，。/没问题"],
        [True, -10, -11, 12.12345, 12.12345678901234, "test01", 11, date(1970, 1, 1), 'Hello, World!'.encode('utf-8'), "string01"],
        [None, None, None, None, None, "", None, date(1970, 1, 1), ''.encode('utf-8'), ""],
        [True, -0, -0, -0.0, -0.0, "    ", 11, date(1970, 1, 1), '    '.encode('utf-8'), "    "],
        [True, 10, 11, 1.1, 10011.1, "test01", 11, date(1970, 1, 1), 'Hello, World!'.encode('utf-8'), "string01"]
    ]
    timestamps_ = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
    tablet4_ = Tablet(device2_id, measurements_, data_types, values_, timestamps_)

    tablet_lst = [tablet1_, tablet2_, tablet3_, tablet4_]
    session.insert_tablets(tablet_lst)
    # Calculate the number of rows in the actual time series
    actual = query("select * from root.test.g1.d1")
    actual = actual + query("select * from root.test.g1.d2")
    actual = actual + query("select * from root.test.g2.d1")
    # Determine whether it meets expectations
    assert expect == actual

    # 2、Inserting Numpy Tablet data
    expect = 40
    device_id = "root.test.g1.d1"
    measurements_ = ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB", "STRING"]
    data_types_ = [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE, TSDataType.TEXT, TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB, TSDataType.STRING]
    np_values_ = [
        np.array([False, True, False, True, False, True, False, True, False, True], TSDataType.BOOLEAN.np_dtype()),
        np.array([0, -2147483648, 2147483647, 1, -1, 10, -10, -0, 10, 10], TSDataType.INT32.np_dtype()),
        np.array([0, -9223372036854775808, 9223372036854775807, 1, -1, 10, -10, -0, 10, 10], TSDataType.INT64.np_dtype()),
        np.array([0.0, -0.12345678, 0.123456789, 1.0, 1.1234567, 4.123456, 12.12345, -0.0, 1.1, 1.1], TSDataType.FLOAT.np_dtype()),
        np.array([0.0, -0.12345678901234567, 0.12345678901234567, 1.0, 1.1234567890123456, 4.123456789012345, 12.12345678901234, -0.0, 1.1, 1.1], TSDataType.DOUBLE.np_dtype()),
        np.array(["1234567890", "abcdefghijklmnopqrstuvwsyz", "!@#$%^&*()_+}{|:'`~-=[];,./<>?~", "没问题", "！@#￥%……&*（）——|：“《》？·【】、；‘，。/", "1234567890abcdefghijklmnopqrstuvwsyz!@#$%^&*()_+}{|:'`~-=[];,./<>?~！@#￥%……&*（）——|：“《》？·【】、；‘，。/没问题", "", "    ", "test01", "test01"], TSDataType.TEXT.np_dtype()),
        np.array([0, -9223372036854775808, 9223372036854775807, 1, -1, 10, -10, -0, 10, 10], TSDataType.TIMESTAMP.np_dtype()),
        np.array([date(1970, 1, 1), date(1000, 1, 1), date(9999, 12, 31), date(1970, 1, 1), date(1970, 1, 1), date(1970, 1, 1), date(1970, 1, 1), date(1970, 1, 1), date(1970, 1, 1), date(1970, 1, 1)], TSDataType.DATE.np_dtype()),
        np.array(['1234567890'.encode('utf-8'), 'abcdefghijklmnopqrstuvwsyz'.encode('utf-8'), '!@#$%^&*()_+}{|:`~-=[];,./<>?~'.encode('utf-8'), '没问题'.encode('utf-8'), '！@#￥%……&*（）——|：“《》？·【】、；‘，。/'.encode('utf-8'), '1234567890abcdefghijklmnopqrstuvwsyz!@#$%^&*()_+}{|:`~-=[];,./<>?~！@#￥%……&*（）——|：“《》？·【】、；‘，。/没问题'.encode('utf-8'), ''.encode('utf-8'), '   '.encode('utf-8'), '1234567890'.encode('utf-8'), '1234567890'.encode('utf-8')], TSDataType.BLOB.np_dtype()),
        np.array(["1234567890", "abcdefghijklmnopqrstuvwsyz", "!@#$%^&*()_+}{|:'`~-=[];,./<>?~", "没问题", "！@#￥%……&*（）——|：“《》？·【】、；‘，。/", "1234567890abcdefghijklmnopqrstuvwsyz!@#$%^&*()_+}{|:'`~-=[];,./<>?~！@#￥%……&*（）——|：“《》？·【】、；‘，。/没问题", "", "    ", "test01", "test01"], TSDataType.STRING.np_dtype())
    ]
    np_timestamps_ = np.array([1, 2, 3, 4, 5, 6, 7, 8, 9, 10], TSDataType.INT64.np_dtype())
    np_tablet1_ = NumpyTablet(device_id, measurements_, data_types_, np_values_, np_timestamps_)
    device_id = "root.test.g1.d1"
    measurements_ = ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB", "STRING"]
    data_types_ = [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE, TSDataType.TEXT, TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB, TSDataType.STRING]
    np_values_ = [
        np.array([False, True, False, True, False, True, False, True, False, True], TSDataType.BOOLEAN.np_dtype()),
        np.array([0, -2147483648, 2147483647, 1, -1, 10, -10, -0, 10, 10], TSDataType.INT32.np_dtype()),
        np.array([0, -9223372036854775808, 9223372036854775807, 1, -1, 10, -10, -0, 10, 10], TSDataType.INT64.np_dtype()),
        np.array([0.0, -0.12345678, 0.123456789, 1.0, 1.1234567, 4.123456, 12.12345, -0.0, 1.1, 1.1], TSDataType.FLOAT.np_dtype()),
        np.array([0.0, -0.12345678901234567, 0.12345678901234567, 1.0, 1.1234567890123456, 4.123456789012345, 12.12345678901234, -0.0, 1.1, 1.1], TSDataType.DOUBLE.np_dtype()),
        np.array(["1234567890", "abcdefghijklmnopqrstuvwsyz", "!@#$%^&*()_+}{|:'`~-=[];,./<>?~", "没问题", "！@#￥%……&*（）——|：“《》？·【】、；‘，。/", "1234567890abcdefghijklmnopqrstuvwsyz!@#$%^&*()_+}{|:'`~-=[];,./<>?~！@#￥%……&*（）——|：“《》？·【】、；‘，。/没问题", "", "    ", "test01", "test01"], TSDataType.TEXT.np_dtype()),
        np.array([0, -9223372036854775808, 9223372036854775807, 1, -1, 10, -10, -0, 10, 10], TSDataType.TIMESTAMP.np_dtype()),
        np.array([date(1970, 1, 1), date(1000, 1, 1), date(9999, 12, 31), date(1970, 1, 1), date(1970, 1, 1), date(1970, 1, 1), date(1970, 1, 1), date(1970, 1, 1), date(1970, 1, 1), date(1970, 1, 1)], TSDataType.DATE.np_dtype()),
        np.array(['1234567890'.encode('utf-8'), 'abcdefghijklmnopqrstuvwsyz'.encode('utf-8'), '!@#$%^&*()_+}{|:`~-=[];,./<>?~'.encode('utf-8'), '没问题'.encode('utf-8'), '！@#￥%……&*（）——|：“《》？·【】、；‘，。/'.encode('utf-8'), '1234567890abcdefghijklmnopqrstuvwsyz!@#$%^&*()_+}{|:`~-=[];,./<>?~！@#￥%……&*（）——|：“《》？·【】、；‘，。/没问题'.encode('utf-8'), ''.encode('utf-8'), '   '.encode('utf-8'), '1234567890'.encode('utf-8'), '1234567890'.encode('utf-8')], TSDataType.BLOB.np_dtype()),
        np.array(["1234567890", "abcdefghijklmnopqrstuvwsyz", "!@#$%^&*()_+}{|:'`~-=[];,./<>?~", "没问题", "！@#￥%……&*（）——|：“《》？·【】、；‘，。/", "1234567890abcdefghijklmnopqrstuvwsyz!@#$%^&*()_+}{|:'`~-=[];,./<>?~！@#￥%……&*（）——|：“《》？·【】、；‘，。/没问题", "", "    ", "test01", "test01"], TSDataType.STRING.np_dtype())
    ]
    np_timestamps_ = np.array([11, 12, 13, 14, 15, 16, 17, 18, 19, 20], TSDataType.INT64.np_dtype())
    np_tablet2_ = NumpyTablet(device_id, measurements_, data_types_, np_values_, np_timestamps_)

    device_id = "root.test.g1.d2"
    measurements_ = ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB", "STRING"]
    data_types_ = [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE, TSDataType.TEXT, TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB, TSDataType.STRING]
    np_values_ = [
        np.array([False, True, False, True, False, True, False, True, False, True], TSDataType.BOOLEAN.np_dtype()),
        np.array([0, -2147483648, 2147483647, 1, -1, 10, -10, -0, 10, 10], TSDataType.INT32.np_dtype()),
        np.array([0, -9223372036854775808, 9223372036854775807, 1, -1, 10, -10, -0, 10, 10], TSDataType.INT64.np_dtype()),
        np.array([0.0, -0.12345678, 0.123456789, 1.0, 1.1234567, 4.123456, 12.12345, -0.0, 1.1, 1.1], TSDataType.FLOAT.np_dtype()),
        np.array([0.0, -0.12345678901234567, 0.12345678901234567, 1.0, 1.1234567890123456, 4.123456789012345, 12.12345678901234, -0.0, 1.1, 1.1], TSDataType.DOUBLE.np_dtype()),
        np.array(["1234567890", "abcdefghijklmnopqrstuvwsyz", "!@#$%^&*()_+}{|:'`~-=[];,./<>?~", "没问题", "！@#￥%……&*（）——|：“《》？·【】、；‘，。/", "1234567890abcdefghijklmnopqrstuvwsyz!@#$%^&*()_+}{|:'`~-=[];,./<>?~！@#￥%……&*（）——|：“《》？·【】、；‘，。/没问题", "", "    ", "test01", "test01"], TSDataType.TEXT.np_dtype()),
        np.array([0, -9223372036854775808, 9223372036854775807, 1, -1, 10, -10, -0, 10, 10], TSDataType.TIMESTAMP.np_dtype()),
        np.array([date(1970, 1, 1), date(1000, 1, 1), date(9999, 12, 31), date(1970, 1, 1), date(1970, 1, 1), date(1970, 1, 1), date(1970, 1, 1), date(1970, 1, 1), date(1970, 1, 1), date(1970, 1, 1)], TSDataType.DATE.np_dtype()),
        np.array(['1234567890'.encode('utf-8'), 'abcdefghijklmnopqrstuvwsyz'.encode('utf-8'), '!@#$%^&*()_+}{|:`~-=[];,./<>?~'.encode('utf-8'), '没问题'.encode('utf-8'), '！@#￥%……&*（）——|：“《》？·【】、；‘，。/'.encode('utf-8'), '1234567890abcdefghijklmnopqrstuvwsyz!@#$%^&*()_+}{|:`~-=[];,./<>?~！@#￥%……&*（）——|：“《》？·【】、；‘，。/没问题'.encode('utf-8'), ''.encode('utf-8'), '   '.encode('utf-8'), '1234567890'.encode('utf-8'), '1234567890'.encode('utf-8')], TSDataType.BLOB.np_dtype()),
        np.array(["1234567890", "abcdefghijklmnopqrstuvwsyz", "!@#$%^&*()_+}{|:'`~-=[];,./<>?~", "没问题", "！@#￥%……&*（）——|：“《》？·【】、；‘，。/", "1234567890abcdefghijklmnopqrstuvwsyz!@#$%^&*()_+}{|:'`~-=[];,./<>?~！@#￥%……&*（）——|：“《》？·【】、；‘，。/没问题", "", "    ", "test01", "test01"], TSDataType.STRING.np_dtype())
    ]
    np_timestamps_ = np.array([1, 2, 3, 4, 5, 6, 7, 8, 9, 10], TSDataType.INT64.np_dtype())
    np_tablet3_ = NumpyTablet(device_id, measurements_, data_types_, np_values_, np_timestamps_)

    device_id = "root.test.g2.d1"
    measurements_ = ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB", "STRING"]
    data_types_ = [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE, TSDataType.TEXT, TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB, TSDataType.STRING]
    np_values_ = [
        np.array([False, True, False, True, False, True, False, True, False, True], TSDataType.BOOLEAN.np_dtype()),
        np.array([0, -2147483648, 2147483647, 1, -1, 10, -10, -0, 10, 10], TSDataType.INT32.np_dtype()),
        np.array([0, -9223372036854775808, 9223372036854775807, 1, -1, 10, -10, -0, 10, 10], TSDataType.INT64.np_dtype()),
        np.array([0.0, -0.12345678, 0.123456789, 1.0, 1.1234567, 4.123456, 12.12345, -0.0, 1.1, 1.1], TSDataType.FLOAT.np_dtype()),
        np.array([0.0, -0.12345678901234567, 0.12345678901234567, 1.0, 1.1234567890123456, 4.123456789012345, 12.12345678901234, -0.0, 1.1, 1.1], TSDataType.DOUBLE.np_dtype()),
        np.array(["1234567890", "abcdefghijklmnopqrstuvwsyz", "!@#$%^&*()_+}{|:'`~-=[];,./<>?~", "没问题", "！@#￥%……&*（）——|：“《》？·【】、；‘，。/", "1234567890abcdefghijklmnopqrstuvwsyz!@#$%^&*()_+}{|:'`~-=[];,./<>?~！@#￥%……&*（）——|：“《》？·【】、；‘，。/没问题", "", "    ", "test01", "test01"], TSDataType.TEXT.np_dtype()),
        np.array([0, -9223372036854775808, 9223372036854775807, 1, -1, 10, -10, -0, 10, 10], TSDataType.TIMESTAMP.np_dtype()),
        np.array([date(1970, 1, 1), date(1000, 1, 1), date(9999, 12, 31), date(1970, 1, 1), date(1970, 1, 1), date(1970, 1, 1), date(1970, 1, 1), date(1970, 1, 1), date(1970, 1, 1), date(1970, 1, 1)], TSDataType.DATE.np_dtype()),
        np.array(['1234567890'.encode('utf-8'), 'abcdefghijklmnopqrstuvwsyz'.encode('utf-8'), '!@#$%^&*()_+}{|:`~-=[];,./<>?~'.encode('utf-8'), '没问题'.encode('utf-8'), '！@#￥%……&*（）——|：“《》？·【】、；‘，。/'.encode('utf-8'), '1234567890abcdefghijklmnopqrstuvwsyz!@#$%^&*()_+}{|:`~-=[];,./<>?~！@#￥%……&*（）——|：“《》？·【】、；‘，。/没问题'.encode('utf-8'), ''.encode('utf-8'), '   '.encode('utf-8'), '1234567890'.encode('utf-8'), '1234567890'.encode('utf-8')], TSDataType.BLOB.np_dtype()),
        np.array(["1234567890", "abcdefghijklmnopqrstuvwsyz", "!@#$%^&*()_+}{|:'`~-=[];,./<>?~", "没问题", "！@#￥%……&*（）——|：“《》？·【】、；‘，。/", "1234567890abcdefghijklmnopqrstuvwsyz!@#$%^&*()_+}{|:'`~-=[];,./<>?~！@#￥%……&*（）——|：“《》？·【】、；‘，。/没问题", "", "    ", "test01", "test01"], TSDataType.STRING.np_dtype())
    ]
    np_timestamps_ = np.array([1, 2, 3, 4, 5, 6, 7, 8, 9, 10], TSDataType.INT64.np_dtype())
    np_tablet4_ = NumpyTablet(device_id, measurements_, data_types_, np_values_, np_timestamps_)

    np_tablet_list = [np_tablet1_, np_tablet2_, np_tablet3_, np_tablet4_]
    session.insert_tablets(np_tablet_list)
    # Calculate the number of rows in the actual time series
    actual = query("select * from root.test.g1.d1")
    actual = actual + query("select * from root.test.g1.d2")
    actual = actual + query("select * from root.test.g2.d1")
    # Determine whether it meets expectations
    assert expect == actual

# Test writing multiple tablet data to aligned time series
@pytest.mark.usefixtures('fixture_')
def test_insert_aligned_tablets():
    global session
    # 1、Inserting tablet data
    expect = 40
    device1_id = "root.test.g3.d1"
    measurements_ = ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB", "STRING"]
    data_types = [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE, TSDataType.TEXT, TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB, TSDataType.STRING]
    values_ = [
        [False, 0, 0, 0.0, 0.0, "1234567890", 0, date(1970, 1, 1), '1234567890'.encode('utf-8'), "1234567890"],
        [True, -2147483648, -9223372036854775808, -0.12345678, -0.12345678901234567, "abcdefghijklmnopqrstuvwsyz", -9223372036854775808, date(1000, 1, 1), 'abcdefghijklmnopqrstuvwsyz'.encode('utf-8'), "abcdefghijklmnopqrstuvwsyz"],
        [True, 2147483647, 9223372036854775807, 0.123456789, 0.12345678901234567, "!@#$%^&*()_+}{|:'`~-=[];,./<>?~", 9223372036854775807, date(9999, 12, 31), '!@#$%^&*()_+}{|:`~-=[];,./<>?~'.encode('utf-8'), "!@#$%^&*()_+}{|:`~-=[];,./<>?~"],
        [True, 1, 1, 1.0, 1.0, "没问题", 1, date(1970, 1, 1), '没问题'.encode('utf-8'), "没问题"],
        [True, -1, -1, 1.1234567, 1.1234567890123456, "！@#￥%……&*（）——|：“《》？·【】、；‘，。/", 11, date(1970, 1, 1), '！@#￥%……&*（）——|：“《》？·【】、；‘，。/'.encode('utf-8'), "！@#￥%……&*（）——|：“《》？·【】、；‘，。/"],
        [True, 10, 11, 4.123456, 4.123456789012345, "1234567890abcdefghijklmnopqrstuvwsyz!@#$%^&*()_+}{|:'`~-=[];,./<>?~！@#￥%……&*（）——|：“《》？·【】、；‘，。/没问题", 11, date(1970, 1, 1), '1234567890abcdefghijklmnopqrstuvwsyz!@#$%^&*()_+}{|:`~-=[];,./<>?~！@#￥%……&*（）——|：“《》？·【】、；‘，。/没问题'.encode('utf-8'), "1234567890abcdefghijklmnopqrstuvwsyz!@#$%^&*()_+}{|:`~-=[];,./<>?~！@#￥%……&*（）——|：“《》？·【】、；‘，。/没问题"],
        [True, -10, -11, 12.12345, 12.12345678901234, "test01", 11, date(1970, 1, 1), 'Hello, World!'.encode('utf-8'), "string01"],
        [None, None, None, None, None, "", None, date(1970, 1, 1), ''.encode('utf-8'), ""],
        [True, -0, -0, -0.0, -0.0, "    ", 11, date(1970, 1, 1), '    '.encode('utf-8'), "    "],
        [True, 10, 11, 1.1, 10011.1, "test01", 11, date(1970, 1, 1), 'Hello, World!'.encode('utf-8'), "string01"]
    ]
    timestamps_ = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
    tablet1_ = Tablet(device1_id, measurements_, data_types, values_, timestamps_)
    device1_id = "root.test.g3.d1"
    measurements_ = ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB", "STRING"]
    data_types = [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE, TSDataType.TEXT, TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB, TSDataType.STRING]
    values_ = [
        [False, 0, 0, 0.0, 0.0, "1234567890", 0, date(1970, 1, 1), '1234567890'.encode('utf-8'), "1234567890"],
        [True, -2147483648, -9223372036854775808, -0.12345678, -0.12345678901234567, "abcdefghijklmnopqrstuvwsyz", -9223372036854775808, date(1000, 1, 1), 'abcdefghijklmnopqrstuvwsyz'.encode('utf-8'), "abcdefghijklmnopqrstuvwsyz"],
        [True, 2147483647, 9223372036854775807, 0.123456789, 0.12345678901234567, "!@#$%^&*()_+}{|:'`~-=[];,./<>?~", 9223372036854775807, date(9999, 12, 31), '!@#$%^&*()_+}{|:`~-=[];,./<>?~'.encode('utf-8'), "!@#$%^&*()_+}{|:`~-=[];,./<>?~"],
        [True, 1, 1, 1.0, 1.0, "没问题", 1, date(1970, 1, 1), '没问题'.encode('utf-8'), "没问题"],
        [True, -1, -1, 1.1234567, 1.1234567890123456, "！@#￥%……&*（）——|：“《》？·【】、；‘，。/", 11, date(1970, 1, 1), '！@#￥%……&*（）——|：“《》？·【】、；‘，。/'.encode('utf-8'), "！@#￥%……&*（）——|：“《》？·【】、；‘，。/"],
        [True, 10, 11, 4.123456, 4.123456789012345, "1234567890abcdefghijklmnopqrstuvwsyz!@#$%^&*()_+}{|:'`~-=[];,./<>?~！@#￥%……&*（）——|：“《》？·【】、；‘，。/没问题", 11, date(1970, 1, 1), '1234567890abcdefghijklmnopqrstuvwsyz!@#$%^&*()_+}{|:`~-=[];,./<>?~！@#￥%……&*（）——|：“《》？·【】、；‘，。/没问题'.encode('utf-8'), "1234567890abcdefghijklmnopqrstuvwsyz!@#$%^&*()_+}{|:`~-=[];,./<>?~！@#￥%……&*（）——|：“《》？·【】、；‘，。/没问题"],
        [True, -10, -11, 12.12345, 12.12345678901234, "test01", 11, date(1970, 1, 1), 'Hello, World!'.encode('utf-8'), "string01"],
        [None, None, None, None, None, "", None, date(1970, 1, 1), ''.encode('utf-8'), ""],
        [True, -0, -0, -0.0, -0.0, "    ", 11, date(1970, 1, 1), '    '.encode('utf-8'), "    "],
        [True, 10, 11, 1.1, 10011.1, "test01", 11, date(1970, 1, 1), 'Hello, World!'.encode('utf-8'), "string01"]
    ]
    timestamps_ = [11, 12, 13, 14, 15, 16, 17, 18, 19, 20]
    tablet2_ = Tablet(device1_id, measurements_, data_types, values_, timestamps_)

    tablet_lst = [tablet1_, tablet2_]
    session.insert_aligned_tablets(tablet_lst)

    device2_id = "root.test.g3.d2"
    measurements_ = ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB", "STRING"]
    data_types = [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE, TSDataType.TEXT, TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB, TSDataType.STRING]
    values_ = [
        [False, 0, 0, 0.0, 0.0, "1234567890", 0, date(1970, 1, 1), '1234567890'.encode('utf-8'), "1234567890"],
        [True, -2147483648, -9223372036854775808, -0.12345678, -0.12345678901234567, "abcdefghijklmnopqrstuvwsyz", -9223372036854775808, date(1000, 1, 1), 'abcdefghijklmnopqrstuvwsyz'.encode('utf-8'), "abcdefghijklmnopqrstuvwsyz"],
        [True, 2147483647, 9223372036854775807, 0.123456789, 0.12345678901234567, "!@#$%^&*()_+}{|:'`~-=[];,./<>?~", 9223372036854775807, date(9999, 12, 31), '!@#$%^&*()_+}{|:`~-=[];,./<>?~'.encode('utf-8'), "!@#$%^&*()_+}{|:`~-=[];,./<>?~"],
        [True, 1, 1, 1.0, 1.0, "没问题", 1, date(1970, 1, 1), '没问题'.encode('utf-8'), "没问题"],
        [True, -1, -1, 1.1234567, 1.1234567890123456, "！@#￥%……&*（）——|：“《》？·【】、；‘，。/", 11, date(1970, 1, 1), '！@#￥%……&*（）——|：“《》？·【】、；‘，。/'.encode('utf-8'), "！@#￥%……&*（）——|：“《》？·【】、；‘，。/"],
        [True, 10, 11, 4.123456, 4.123456789012345, "1234567890abcdefghijklmnopqrstuvwsyz!@#$%^&*()_+}{|:'`~-=[];,./<>?~！@#￥%……&*（）——|：“《》？·【】、；‘，。/没问题", 11, date(1970, 1, 1), '1234567890abcdefghijklmnopqrstuvwsyz!@#$%^&*()_+}{|:`~-=[];,./<>?~！@#￥%……&*（）——|：“《》？·【】、；‘，。/没问题'.encode('utf-8'), "1234567890abcdefghijklmnopqrstuvwsyz!@#$%^&*()_+}{|:`~-=[];,./<>?~！@#￥%……&*（）——|：“《》？·【】、；‘，。/没问题"],
        [True, -10, -11, 12.12345, 12.12345678901234, "test01", 11, date(1970, 1, 1), 'Hello, World!'.encode('utf-8'), "string01"],
        [None, None, None, None, None, "", None, date(1970, 1, 1), ''.encode('utf-8'), ""],
        [True, -0, -0, -0.0, -0.0, "    ", 11, date(1970, 1, 1), '    '.encode('utf-8'), "    "],
        [True, 10, 11, 1.1, 10011.1, "test01", 11, date(1970, 1, 1), 'Hello, World!'.encode('utf-8'), "string01"]
    ]
    timestamps_ = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
    tablet3_ = Tablet(device2_id, measurements_, data_types, values_, timestamps_)

    device2_id = "root.test.g4.d1"
    measurements_ = ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB", "STRING"]
    data_types = [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE, TSDataType.TEXT, TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB, TSDataType.STRING]
    values_ = [
        [False, 0, 0, 0.0, 0.0, "1234567890", 0, date(1970, 1, 1), '1234567890'.encode('utf-8'), "1234567890"],
        [True, -2147483648, -9223372036854775808, -0.12345678, -0.12345678901234567, "abcdefghijklmnopqrstuvwsyz", -9223372036854775808, date(1000, 1, 1), 'abcdefghijklmnopqrstuvwsyz'.encode('utf-8'), "abcdefghijklmnopqrstuvwsyz"],
        [True, 2147483647, 9223372036854775807, 0.123456789, 0.12345678901234567, "!@#$%^&*()_+}{|:'`~-=[];,./<>?~", 9223372036854775807, date(9999, 12, 31), '!@#$%^&*()_+}{|:`~-=[];,./<>?~'.encode('utf-8'), "!@#$%^&*()_+}{|:`~-=[];,./<>?~"],
        [True, 1, 1, 1.0, 1.0, "没问题", 1, date(1970, 1, 1), '没问题'.encode('utf-8'), "没问题"],
        [True, -1, -1, 1.1234567, 1.1234567890123456, "！@#￥%……&*（）——|：“《》？·【】、；‘，。/", 11, date(1970, 1, 1), '！@#￥%……&*（）——|：“《》？·【】、；‘，。/'.encode('utf-8'), "！@#￥%……&*（）——|：“《》？·【】、；‘，。/"],
        [True, 10, 11, 4.123456, 4.123456789012345, "1234567890abcdefghijklmnopqrstuvwsyz!@#$%^&*()_+}{|:'`~-=[];,./<>?~！@#￥%……&*（）——|：“《》？·【】、；‘，。/没问题", 11, date(1970, 1, 1), '1234567890abcdefghijklmnopqrstuvwsyz!@#$%^&*()_+}{|:`~-=[];,./<>?~！@#￥%……&*（）——|：“《》？·【】、；‘，。/没问题'.encode('utf-8'), "1234567890abcdefghijklmnopqrstuvwsyz!@#$%^&*()_+}{|:`~-=[];,./<>?~！@#￥%……&*（）——|：“《》？·【】、；‘，。/没问题"],
        [True, -10, -11, 12.12345, 12.12345678901234, "test01", 11, date(1970, 1, 1), 'Hello, World!'.encode('utf-8'), "string01"],
        [None, None, None, None, None, "", None, date(1970, 1, 1), ''.encode('utf-8'), ""],
        [True, -0, -0, -0.0, -0.0, "    ", 11, date(1970, 1, 1), '    '.encode('utf-8'), "    "],
        [True, 10, 11, 1.1, 10011.1, "test01", 11, date(1970, 1, 1), 'Hello, World!'.encode('utf-8'), "string01"]
    ]
    timestamps_ = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
    tablet4_ = Tablet(device2_id, measurements_, data_types, values_, timestamps_)

    tablet_lst = [tablet1_, tablet2_, tablet3_, tablet4_]
    session.insert_aligned_tablets(tablet_lst)
    # Calculate the number of rows in the actual time series
    actual = query("select * from root.test.g3.d1")
    actual = actual + query("select * from root.test.g3.d2")
    actual = actual + query("select * from root.test.g4.d1")
    # Determine whether it meets expectations
    assert expect == actual

    # 2、Inserting Numpy Tablet data
    expect = 40
    device_id = "root.test.g1.d1"
    measurements_ = ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB", "STRING"]
    data_types_ = [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE, TSDataType.TEXT, TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB, TSDataType.STRING]
    np_values_ = [
        np.array([False, True, False, True, False, True, False, True, False, True], TSDataType.BOOLEAN.np_dtype()),
        np.array([0, -2147483648, 2147483647, 1, -1, 10, -10, -0, 10, 10], TSDataType.INT32.np_dtype()),
        np.array([0, -9223372036854775808, 9223372036854775807, 1, -1, 10, -10, -0, 10, 10], TSDataType.INT64.np_dtype()),
        np.array([0.0, -0.12345678, 0.123456789, 1.0, 1.1234567, 4.123456, 12.12345, -0.0, 1.1, 1.1], TSDataType.FLOAT.np_dtype()),
        np.array([0.0, -0.12345678901234567, 0.12345678901234567, 1.0, 1.1234567890123456, 4.123456789012345, 12.12345678901234, -0.0, 1.1, 1.1], TSDataType.DOUBLE.np_dtype()),
        np.array(["1234567890", "abcdefghijklmnopqrstuvwsyz", "!@#$%^&*()_+}{|:'`~-=[];,./<>?~", "没问题", "！@#￥%……&*（）——|：“《》？·【】、；‘，。/", "1234567890abcdefghijklmnopqrstuvwsyz!@#$%^&*()_+}{|:'`~-=[];,./<>?~！@#￥%……&*（）——|：“《》？·【】、；‘，。/没问题", "", "    ", "test01", "test01"], TSDataType.TEXT.np_dtype()),
        np.array([0, -9223372036854775808, 9223372036854775807, 1, -1, 10, -10, -0, 10, 10], TSDataType.TIMESTAMP.np_dtype()),
        np.array([date(1970, 1, 1), date(1000, 1, 1), date(9999, 12, 31), date(1970, 1, 1), date(1970, 1, 1), date(1970, 1, 1), date(1970, 1, 1), date(1970, 1, 1), date(1970, 1, 1), date(1970, 1, 1)], TSDataType.DATE.np_dtype()),
        np.array(['1234567890'.encode('utf-8'), 'abcdefghijklmnopqrstuvwsyz'.encode('utf-8'), '!@#$%^&*()_+}{|:`~-=[];,./<>?~'.encode('utf-8'), '没问题'.encode('utf-8'), '！@#￥%……&*（）——|：“《》？·【】、；‘，。/'.encode('utf-8'), '1234567890abcdefghijklmnopqrstuvwsyz!@#$%^&*()_+}{|:`~-=[];,./<>?~！@#￥%……&*（）——|：“《》？·【】、；‘，。/没问题'.encode('utf-8'), ''.encode('utf-8'), '   '.encode('utf-8'), '1234567890'.encode('utf-8'), '1234567890'.encode('utf-8')], TSDataType.BLOB.np_dtype()),
        np.array(["1234567890", "abcdefghijklmnopqrstuvwsyz", "!@#$%^&*()_+}{|:'`~-=[];,./<>?~", "没问题", "！@#￥%……&*（）——|：“《》？·【】、；‘，。/", "1234567890abcdefghijklmnopqrstuvwsyz!@#$%^&*()_+}{|:'`~-=[];,./<>?~！@#￥%……&*（）——|：“《》？·【】、；‘，。/没问题", "", "    ", "test01", "test01"], TSDataType.STRING.np_dtype())
    ]
    np_timestamps_ = np.array([1, 2, 3, 4, 5, 6, 7, 8, 9, 10], TSDataType.INT64.np_dtype())
    np_tablet1_ = NumpyTablet(device_id, measurements_, data_types_, np_values_, np_timestamps_)
    device_id = "root.test.g1.d1"
    measurements_ = ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB", "STRING"]
    data_types_ = [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE, TSDataType.TEXT, TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB, TSDataType.STRING]
    np_values_ = [
        np.array([False, True, False, True, False, True, False, True, False, True], TSDataType.BOOLEAN.np_dtype()),
        np.array([0, -2147483648, 2147483647, 1, -1, 10, -10, -0, 10, 10], TSDataType.INT32.np_dtype()),
        np.array([0, -9223372036854775808, 9223372036854775807, 1, -1, 10, -10, -0, 10, 10], TSDataType.INT64.np_dtype()),
        np.array([0.0, -0.12345678, 0.123456789, 1.0, 1.1234567, 4.123456, 12.12345, -0.0, 1.1, 1.1], TSDataType.FLOAT.np_dtype()),
        np.array([0.0, -0.12345678901234567, 0.12345678901234567, 1.0, 1.1234567890123456, 4.123456789012345, 12.12345678901234, -0.0, 1.1, 1.1], TSDataType.DOUBLE.np_dtype()),
        np.array(["1234567890", "abcdefghijklmnopqrstuvwsyz", "!@#$%^&*()_+}{|:'`~-=[];,./<>?~", "没问题", "！@#￥%……&*（）——|：“《》？·【】、；‘，。/", "1234567890abcdefghijklmnopqrstuvwsyz!@#$%^&*()_+}{|:'`~-=[];,./<>?~！@#￥%……&*（）——|：“《》？·【】、；‘，。/没问题", "", "    ", "test01", "test01"], TSDataType.TEXT.np_dtype()),
        np.array([0, -9223372036854775808, 9223372036854775807, 1, -1, 10, -10, -0, 10, 10], TSDataType.TIMESTAMP.np_dtype()),
        np.array([date(1970, 1, 1), date(1000, 1, 1), date(9999, 12, 31), date(1970, 1, 1), date(1970, 1, 1), date(1970, 1, 1), date(1970, 1, 1), date(1970, 1, 1), date(1970, 1, 1), date(1970, 1, 1)], TSDataType.DATE.np_dtype()),
        np.array(['1234567890'.encode('utf-8'), 'abcdefghijklmnopqrstuvwsyz'.encode('utf-8'), '!@#$%^&*()_+}{|:`~-=[];,./<>?~'.encode('utf-8'), '没问题'.encode('utf-8'), '！@#￥%……&*（）——|：“《》？·【】、；‘，。/'.encode('utf-8'), '1234567890abcdefghijklmnopqrstuvwsyz!@#$%^&*()_+}{|:`~-=[];,./<>?~！@#￥%……&*（）——|：“《》？·【】、；‘，。/没问题'.encode('utf-8'), ''.encode('utf-8'), '   '.encode('utf-8'), '1234567890'.encode('utf-8'), '1234567890'.encode('utf-8')], TSDataType.BLOB.np_dtype()),
        np.array(["1234567890", "abcdefghijklmnopqrstuvwsyz", "!@#$%^&*()_+}{|:'`~-=[];,./<>?~", "没问题", "！@#￥%……&*（）——|：“《》？·【】、；‘，。/", "1234567890abcdefghijklmnopqrstuvwsyz!@#$%^&*()_+}{|:'`~-=[];,./<>?~！@#￥%……&*（）——|：“《》？·【】、；‘，。/没问题", "", "    ", "test01", "test01"], TSDataType.STRING.np_dtype())
    ]
    np_timestamps_ = np.array([11, 12, 13, 14, 15, 16, 17, 18, 19, 20], TSDataType.INT64.np_dtype())
    np_tablet2_ = NumpyTablet(device_id, measurements_, data_types_, np_values_, np_timestamps_)

    device_id = "root.test.g1.d2"
    measurements_ = ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB", "STRING"]
    data_types_ = [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE, TSDataType.TEXT, TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB, TSDataType.STRING]
    np_values_ = [
        np.array([False, True, False, True, False, True, False, True, False, True], TSDataType.BOOLEAN.np_dtype()),
        np.array([0, -2147483648, 2147483647, 1, -1, 10, -10, -0, 10, 10], TSDataType.INT32.np_dtype()),
        np.array([0, -9223372036854775808, 9223372036854775807, 1, -1, 10, -10, -0, 10, 10], TSDataType.INT64.np_dtype()),
        np.array([0.0, -0.12345678, 0.123456789, 1.0, 1.1234567, 4.123456, 12.12345, -0.0, 1.1, 1.1], TSDataType.FLOAT.np_dtype()),
        np.array([0.0, -0.12345678901234567, 0.12345678901234567, 1.0, 1.1234567890123456, 4.123456789012345, 12.12345678901234, -0.0, 1.1, 1.1], TSDataType.DOUBLE.np_dtype()),
        np.array(["1234567890", "abcdefghijklmnopqrstuvwsyz", "!@#$%^&*()_+}{|:'`~-=[];,./<>?~", "没问题", "！@#￥%……&*（）——|：“《》？·【】、；‘，。/", "1234567890abcdefghijklmnopqrstuvwsyz!@#$%^&*()_+}{|:'`~-=[];,./<>?~！@#￥%……&*（）——|：“《》？·【】、；‘，。/没问题", "", "    ", "test01", "test01"], TSDataType.TEXT.np_dtype()),
        np.array([0, -9223372036854775808, 9223372036854775807, 1, -1, 10, -10, -0, 10, 10], TSDataType.TIMESTAMP.np_dtype()),
        np.array([date(1970, 1, 1), date(1000, 1, 1), date(9999, 12, 31), date(1970, 1, 1), date(1970, 1, 1), date(1970, 1, 1), date(1970, 1, 1), date(1970, 1, 1), date(1970, 1, 1), date(1970, 1, 1)], TSDataType.DATE.np_dtype()),
        np.array(['1234567890'.encode('utf-8'), 'abcdefghijklmnopqrstuvwsyz'.encode('utf-8'), '!@#$%^&*()_+}{|:`~-=[];,./<>?~'.encode('utf-8'), '没问题'.encode('utf-8'), '！@#￥%……&*（）——|：“《》？·【】、；‘，。/'.encode('utf-8'), '1234567890abcdefghijklmnopqrstuvwsyz!@#$%^&*()_+}{|:`~-=[];,./<>?~！@#￥%……&*（）——|：“《》？·【】、；‘，。/没问题'.encode('utf-8'), ''.encode('utf-8'), '   '.encode('utf-8'), '1234567890'.encode('utf-8'), '1234567890'.encode('utf-8')], TSDataType.BLOB.np_dtype()),
        np.array(["1234567890", "abcdefghijklmnopqrstuvwsyz", "!@#$%^&*()_+}{|:'`~-=[];,./<>?~", "没问题", "！@#￥%……&*（）——|：“《》？·【】、；‘，。/", "1234567890abcdefghijklmnopqrstuvwsyz!@#$%^&*()_+}{|:'`~-=[];,./<>?~！@#￥%……&*（）——|：“《》？·【】、；‘，。/没问题", "", "    ", "test01", "test01"], TSDataType.STRING.np_dtype())
    ]
    np_timestamps_ = np.array([1, 2, 3, 4, 5, 6, 7, 8, 9, 10], TSDataType.INT64.np_dtype())
    np_tablet3_ = NumpyTablet(device_id, measurements_, data_types_, np_values_, np_timestamps_)

    device_id = "root.test.g2.d1"
    measurements_ = ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB", "STRING"]
    data_types_ = [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE, TSDataType.TEXT, TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB, TSDataType.STRING]
    np_values_ = [
       np.array([False, True, False, True, False, True, False, True, False, True], TSDataType.BOOLEAN.np_dtype()),
        np.array([0, -2147483648, 2147483647, 1, -1, 10, -10, -0, 10, 10], TSDataType.INT32.np_dtype()),
        np.array([0, -9223372036854775808, 9223372036854775807, 1, -1, 10, -10, -0, 10, 10], TSDataType.INT64.np_dtype()),
        np.array([0.0, -0.12345678, 0.123456789, 1.0, 1.1234567, 4.123456, 12.12345, -0.0, 1.1, 1.1], TSDataType.FLOAT.np_dtype()),
        np.array([0.0, -0.12345678901234567, 0.12345678901234567, 1.0, 1.1234567890123456, 4.123456789012345, 12.12345678901234, -0.0, 1.1, 1.1], TSDataType.DOUBLE.np_dtype()),
        np.array(["1234567890", "abcdefghijklmnopqrstuvwsyz", "!@#$%^&*()_+}{|:'`~-=[];,./<>?~", "没问题", "！@#￥%……&*（）——|：“《》？·【】、；‘，。/", "1234567890abcdefghijklmnopqrstuvwsyz!@#$%^&*()_+}{|:'`~-=[];,./<>?~！@#￥%……&*（）——|：“《》？·【】、；‘，。/没问题", "", "    ", "test01", "test01"], TSDataType.TEXT.np_dtype()),
        np.array([0, -9223372036854775808, 9223372036854775807, 1, -1, 10, -10, -0, 10, 10], TSDataType.TIMESTAMP.np_dtype()),
        np.array([date(1970, 1, 1), date(1000, 1, 1), date(9999, 12, 31), date(1970, 1, 1), date(1970, 1, 1), date(1970, 1, 1), date(1970, 1, 1), date(1970, 1, 1), date(1970, 1, 1), date(1970, 1, 1)], TSDataType.DATE.np_dtype()),
        np.array(['1234567890'.encode('utf-8'), 'abcdefghijklmnopqrstuvwsyz'.encode('utf-8'), '!@#$%^&*()_+}{|:`~-=[];,./<>?~'.encode('utf-8'), '没问题'.encode('utf-8'), '！@#￥%……&*（）——|：“《》？·【】、；‘，。/'.encode('utf-8'), '1234567890abcdefghijklmnopqrstuvwsyz!@#$%^&*()_+}{|:`~-=[];,./<>?~！@#￥%……&*（）——|：“《》？·【】、；‘，。/没问题'.encode('utf-8'), ''.encode('utf-8'), '   '.encode('utf-8'), '1234567890'.encode('utf-8'), '1234567890'.encode('utf-8')], TSDataType.BLOB.np_dtype()),
        np.array(["1234567890", "abcdefghijklmnopqrstuvwsyz", "!@#$%^&*()_+}{|:'`~-=[];,./<>?~", "没问题", "！@#￥%……&*（）——|：“《》？·【】、；‘，。/", "1234567890abcdefghijklmnopqrstuvwsyz!@#$%^&*()_+}{|:'`~-=[];,./<>?~！@#￥%……&*（）——|：“《》？·【】、；‘，。/没问题", "", "    ", "test01", "test01"], TSDataType.STRING.np_dtype())
    ]
    np_timestamps_ = np.array([1, 2, 3, 4, 5, 6, 7, 8, 9, 10], TSDataType.INT64.np_dtype())
    np_tablet4_ = NumpyTablet(device_id, measurements_, data_types_, np_values_, np_timestamps_)

    np_tablet_list = [np_tablet1_, np_tablet2_, np_tablet3_, np_tablet4_]
    session.insert_aligned_tablets(np_tablet_list)
    # Calculate the number of rows in the actual time series
    actual = query("select * from root.test.g3.d1")
    actual = actual + query("select * from root.test.g3.d2")
    actual = actual + query("select * from root.test.g4.d1")
    # Determine whether it meets expectations
    assert expect == actual

# Test writing a Record data to a non aligned time series
@pytest.mark.usefixtures('fixture_')
def test_insert_record():
    global session
    expect = 9
    session.insert_record("root.test.g1.d1", 1, 
                          ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB", "STRING"], 
                          [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE, TSDataType.TEXT, TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB, TSDataType.STRING], 
                          [False, 0, 0, 0.0, 0.0, "1234567890", 0, date(1970, 1, 1), '1234567890'.encode('utf-8'), "1234567890"])
    session.insert_record("root.test.g1.d1", 2, 
                          ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB", "STRING"], 
                          [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE, TSDataType.TEXT, TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB, TSDataType.STRING], 
                          [True, -2147483648, -9223372036854775808, -0.12345678, -0.12345678901234567, "abcdefghijklmnopqrstuvwsyz", -9223372036854775808, date(1000, 1, 1), 'abcdefghijklmnopqrstuvwsyz'.encode('utf-8'), "abcdefghijklmnopqrstuvwsyz"])
    session.insert_record("root.test.g1.d1", 3, 
                          ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB", "STRING"], 
                          [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE, TSDataType.TEXT, TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB, TSDataType.STRING], 
                          [True, 2147483647, 9223372036854775807, 0.123456789, 0.12345678901234567, "!@#$%^&*()_+}{|:'`~-=[];,./<>?~", 9223372036854775807, date(9999, 12, 31), '!@#$%^&*()_+}{|:`~-=[];,./<>?~'.encode('utf-8'), "!@#$%^&*()_+}{|:`~-=[];,./<>?~"])
    session.insert_record("root.test.g1.d1", 4, 
                          ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB", "STRING"], 
                          [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE, TSDataType.TEXT, TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB, TSDataType.STRING], 
                          [True, 1, 1, 1.0, 1.0, "没问题", 1, date(1970, 1, 1), '没问题'.encode('utf-8'), "没问题"])
    session.insert_record("root.test.g1.d1", 5, 
                          ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB", "STRING"], 
                          [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE, TSDataType.TEXT, TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB, TSDataType.STRING], 
                          [True, -1, -1, 1.1234567, 1.1234567890123456, "！@#￥%……&*（）——|：“《》？·【】、；‘，。/", 11, date(1970, 1, 1), '！@#￥%……&*（）——|：“《》？·【】、；‘，。/'.encode('utf-8'), "！@#￥%……&*（）——|：“《》？·【】、；‘，。/"])
    session.insert_record("root.test.g1.d1", 6, 
                          ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB", "STRING"], 
                          [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE, TSDataType.TEXT, TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB, TSDataType.STRING], 
                          [True, 10, 11, 4.123456, 4.123456789012345, "1234567890abcdefghijklmnopqrstuvwsyz!@#$%^&*()_+}{|:'`~-=[];,./<>?~！@#￥%……&*（）——|：“《》？·【】、；‘，。/没问题", 11, date(1970, 1, 1), '1234567890abcdefghijklmnopqrstuvwsyz!@#$%^&*()_+}{|:`~-=[];,./<>?~！@#￥%……&*（）——|：“《》？·【】、；‘，。/没问题'.encode('utf-8'), "1234567890abcdefghijklmnopqrstuvwsyz!@#$%^&*()_+}{|:`~-=[];,./<>?~！@#￥%……&*（）——|：“《》？·【】、；‘，。/没问题"])
    session.insert_record("root.test.g1.d1", 7, 
                          ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB", "STRING"], 
                          [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE, TSDataType.TEXT, TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB, TSDataType.STRING], 
                          [True, -10, -11, 12.12345, 12.12345678901234, "test01", 11, date(1970, 1, 1), 'Hello, World!'.encode('utf-8'), "string01"])
    session.insert_record("root.test.g1.d1", 9, 
                          ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB", "STRING"], 
                          [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE, TSDataType.TEXT, TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB, TSDataType.STRING], 
                          [True, -0, -0, -0.0, -0.0, "test01", 11, date(1970, 1, 1), 'Hello, World!'.encode('utf-8'), "string01"])
    session.insert_record("root.test.g1.d1", 10, 
                          ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB", "STRING"], 
                          [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE, TSDataType.TEXT, TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB, TSDataType.STRING], 
                          [True, 10, 11, 1.1, 10011.1, "test01", 11, date(1970, 1, 1), 'Hello, World!'.encode('utf-8'), "string01"])
    # Calculate the number of rows in the actual time series
    actual = query("select * from root.test.g1.d1")
    # Determine whether it meets expectations
    assert expect == actual

# Test writing a Record data to align the time series
@pytest.mark.usefixtures('fixture_')
def test_insert_aligned_record():
    global session
    expect = 9
    session.insert_aligned_record("root.test.g3.d1", 1, 
                          ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB", "STRING"], 
                          [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE, TSDataType.TEXT, TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB, TSDataType.STRING], 
                          [False, 0, 0, 0.0, 0.0, "1234567890", 0, date(1970, 1, 1), '1234567890'.encode('utf-8'), "1234567890"])
    session.insert_aligned_record("root.test.g3.d1", 2, 
                          ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB", "STRING"], 
                          [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE, TSDataType.TEXT, TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB, TSDataType.STRING], 
                          [True, -2147483648, -9223372036854775808, -0.12345678, -0.12345678901234567, "abcdefghijklmnopqrstuvwsyz", -9223372036854775808, date(1000, 1, 1), 'abcdefghijklmnopqrstuvwsyz'.encode('utf-8'), "abcdefghijklmnopqrstuvwsyz"])
    session.insert_aligned_record("root.test.g3.d1", 3, 
                          ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB", "STRING"], 
                          [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE, TSDataType.TEXT, TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB, TSDataType.STRING], 
                          [True, 2147483647, 9223372036854775807, 0.123456789, 0.12345678901234567, "!@#$%^&*()_+}{|:'`~-=[];,./<>?~", 9223372036854775807, date(9999, 12, 31), '!@#$%^&*()_+}{|:`~-=[];,./<>?~'.encode('utf-8'), "!@#$%^&*()_+}{|:`~-=[];,./<>?~"])
    session.insert_aligned_record("root.test.g3.d1", 4, 
                          ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB", "STRING"], 
                          [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE, TSDataType.TEXT, TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB, TSDataType.STRING], 
                          [True, 1, 1, 1.0, 1.0, "没问题", 1, date(1970, 1, 1), '没问题'.encode('utf-8'), "没问题"])
    session.insert_aligned_record("root.test.g3.d1", 5, 
                          ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB", "STRING"], 
                          [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE, TSDataType.TEXT, TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB, TSDataType.STRING], 
                          [True, -1, -1, 1.1234567, 1.1234567890123456, "！@#￥%……&*（）——|：“《》？·【】、；‘，。/", 11, date(1970, 1, 1), '！@#￥%……&*（）——|：“《》？·【】、；‘，。/'.encode('utf-8'), "！@#￥%……&*（）——|：“《》？·【】、；‘，。/"])
    session.insert_aligned_record("root.test.g3.d1", 6, 
                          ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB", "STRING"], 
                          [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE, TSDataType.TEXT, TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB, TSDataType.STRING], 
                          [True, 10, 11, 4.123456, 4.123456789012345, "1234567890abcdefghijklmnopqrstuvwsyz!@#$%^&*()_+}{|:'`~-=[];,./<>?~！@#￥%……&*（）——|：“《》？·【】、；‘，。/没问题", 11, date(1970, 1, 1), '1234567890abcdefghijklmnopqrstuvwsyz!@#$%^&*()_+}{|:`~-=[];,./<>?~！@#￥%……&*（）——|：“《》？·【】、；‘，。/没问题'.encode('utf-8'), "1234567890abcdefghijklmnopqrstuvwsyz!@#$%^&*()_+}{|:`~-=[];,./<>?~！@#￥%……&*（）——|：“《》？·【】、；‘，。/没问题"])
    session.insert_aligned_record("root.test.g3.d1", 7, 
                          ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB", "STRING"], 
                          [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE, TSDataType.TEXT, TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB, TSDataType.STRING], 
                          [True, -10, -11, 12.12345, 12.12345678901234, "test01", 11, date(1970, 1, 1), 'Hello, World!'.encode('utf-8'), "string01"])
    session.insert_aligned_record("root.test.g3.d1", 9, 
                          ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB", "STRING"], 
                          [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE, TSDataType.TEXT, TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB, TSDataType.STRING], 
                          [True, -0, -0, -0.0, -0.0, "test01", 11, date(1970, 1, 1), 'Hello, World!'.encode('utf-8'), "string01"])
    session.insert_aligned_record("root.test.g3.d1", 10, 
                          ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB", "STRING"], 
                          [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE, TSDataType.TEXT, TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB, TSDataType.STRING], 
                          [True, 10, 11, 1.1, 10011.1, "test01", 11, date(1970, 1, 1), 'Hello, World!'.encode('utf-8'), "string01"])
    # Calculate the number of rows in the actual time series
    actual = query("select * from root.test.g3.d1")
    # Determine whether it meets expectations
    assert expect == actual

# Test writing multiple Record data to non aligned time series
@pytest.mark.usefixtures('fixture_')
def test_insert_records():
    global session
    expect = 9
    session.insert_records(["root.test.g1.d1", "root.test.g1.d2", "root.test.g2.d1",  "root.test.g1.d1", "root.test.g1.d1", "root.test.g1.d1", "root.test.g1.d1", "root.test.g1.d1", "root.test.g1.d1"], 
                           [1, 2, 3, 4, 5, 6, 7, 8, 9], 
                           [["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB", "STRING"],
                            ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB", "STRING"],
                            ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB", "STRING"],
                            ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB", "STRING"],
                            ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB", "STRING"],
                            ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB", "STRING"],
                            ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB", "STRING"],
                            ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB", "STRING"],
                            ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB", "STRING"]], 
                           [[TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE, TSDataType.TEXT, TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB, TSDataType.STRING], 
                            [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE, TSDataType.TEXT, TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB, TSDataType.STRING], 
                            [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE, TSDataType.TEXT, TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB, TSDataType.STRING],
                            [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE, TSDataType.TEXT, TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB, TSDataType.STRING],
                            [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE, TSDataType.TEXT, TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB, TSDataType.STRING],
                            [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE, TSDataType.TEXT, TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB, TSDataType.STRING],
                            [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE, TSDataType.TEXT, TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB, TSDataType.STRING],
                            [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE, TSDataType.TEXT, TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB, TSDataType.STRING],
                            [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE, TSDataType.TEXT, TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB, TSDataType.STRING]], 
                           [[False, 0, 0, 0.0, 0.0, "1234567890", 0, date(1970, 1, 1), '1234567890'.encode('utf-8'), "1234567890"],
                            [True, -2147483648, -9223372036854775808, -0.12345678, -0.12345678901234567, "abcdefghijklmnopqrstuvwsyz", -9223372036854775808, date(1000, 1, 1), 'abcdefghijklmnopqrstuvwsyz'.encode('utf-8'), "abcdefghijklmnopqrstuvwsyz"],
                            [True, 2147483647, 9223372036854775807, 0.123456789, 0.12345678901234567, "!@#$%^&*()_+}{|:'`~-=[];,./<>?~", 9223372036854775807, date(9999, 12, 31), '!@#$%^&*()_+}{|:`~-=[];,./<>?~'.encode('utf-8'), "!@#$%^&*()_+}{|:`~-=[];,./<>?~"],
                            [True, 1, 1, 1.0, 1.0, "没问题", 1, date(1970, 1, 1), '没问题'.encode('utf-8'), "没问题"],
                            [True, -1, -1, 1.1234567, 1.1234567890123456, "！@#￥%……&*（）——|：“《》？·【】、；‘，。/", 11, date(1970, 1, 1), '！@#￥%……&*（）——|：“《》？·【】、；‘，。/'.encode('utf-8'), "！@#￥%……&*（）——|：“《》？·【】、；‘，。/"],
                            [True, 10, 11, 4.123456, 4.123456789012345, "1234567890abcdefghijklmnopqrstuvwsyz!@#$%^&*()_+}{|:'`~-=[];,./<>?~！@#￥%……&*（）——|：“《》？·【】、；‘，。/没问题", 11, date(1970, 1, 1), '1234567890abcdefghijklmnopqrstuvwsyz!@#$%^&*()_+}{|:`~-=[];,./<>?~！@#￥%……&*（）——|：“《》？·【】、；‘，。/没问题'.encode('utf-8'), "1234567890abcdefghijklmnopqrstuvwsyz!@#$%^&*()_+}{|:`~-=[];,./<>?~！@#￥%……&*（）——|：“《》？·【】、；‘，。/没问题"],
                            [True, -10, -11, 12.12345, 12.12345678901234, "test01", 11, date(1970, 1, 1), 'Hello, World!'.encode('utf-8'), "string01"],
                            [True, -0, -0, -0.0, -0.0, "test01", 11, date(1970, 1, 1), 'Hello, World!'.encode('utf-8'), "string01"],
                            [True, 10, 11, 1.1, 10011.1, "test01", 11, date(1970, 1, 1), 'Hello, World!'.encode('utf-8'), "string01"]])
    # Calculate the number of rows in the actual time series
    actual = query("select * from root.test.g1.d1")
    actual = actual + query("select * from root.test.g1.d2")
    actual = actual + query("select * from root.test.g2.d1")
    # Determine whether it meets expectations
    assert expect == actual

# Test writing multiple Record data to aligned time series
@pytest.mark.usefixtures('fixture_')
def test_insert_aligned_records():
    global session
    expect = 9
    session.insert_aligned_records(["root.test.g3.d1", "root.test.g3.d2", "root.test.g4.d1",  "root.test.g3.d1", "root.test.g3.d1", "root.test.g3.d1", "root.test.g3.d1", "root.test.g3.d1", "root.test.g3.d1"], 
                           [1, 2, 3, 4, 5, 6, 7, 8, 9], 
                           [["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB", "STRING"],
                            ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB", "STRING"],
                            ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB", "STRING"],
                            ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB", "STRING"],
                            ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB", "STRING"],
                            ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB", "STRING"],
                            ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB", "STRING"],
                            ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB", "STRING"],
                            ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB", "STRING"]], 
                           [[TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE, TSDataType.TEXT, TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB, TSDataType.STRING], 
                            [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE, TSDataType.TEXT, TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB, TSDataType.STRING], 
                            [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE, TSDataType.TEXT, TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB, TSDataType.STRING],
                            [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE, TSDataType.TEXT, TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB, TSDataType.STRING],
                            [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE, TSDataType.TEXT, TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB, TSDataType.STRING],
                            [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE, TSDataType.TEXT, TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB, TSDataType.STRING],
                            [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE, TSDataType.TEXT, TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB, TSDataType.STRING],
                            [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE, TSDataType.TEXT, TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB, TSDataType.STRING],
                            [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE, TSDataType.TEXT, TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB, TSDataType.STRING]], 
                           [[False, 0, 0, 0.0, 0.0, "1234567890", 0, date(1970, 1, 1), '1234567890'.encode('utf-8'), "1234567890"],
                            [True, -2147483648, -9223372036854775808, -0.12345678, -0.12345678901234567, "abcdefghijklmnopqrstuvwsyz", -9223372036854775808, date(1000, 1, 1), 'abcdefghijklmnopqrstuvwsyz'.encode('utf-8'), "abcdefghijklmnopqrstuvwsyz"],
                            [True, 2147483647, 9223372036854775807, 0.123456789, 0.12345678901234567, "!@#$%^&*()_+}{|:'`~-=[];,./<>?~", 9223372036854775807, date(9999, 12, 31), '!@#$%^&*()_+}{|:`~-=[];,./<>?~'.encode('utf-8'), "!@#$%^&*()_+}{|:`~-=[];,./<>?~"],
                            [True, 1, 1, 1.0, 1.0, "没问题", 1, date(1970, 1, 1), '没问题'.encode('utf-8'), "没问题"],
                            [True, -1, -1, 1.1234567, 1.1234567890123456, "！@#￥%……&*（）——|：“《》？·【】、；‘，。/", 11, date(1970, 1, 1), '！@#￥%……&*（）——|：“《》？·【】、；‘，。/'.encode('utf-8'), "！@#￥%……&*（）——|：“《》？·【】、；‘，。/"],
                            [True, 10, 11, 4.123456, 4.123456789012345, "1234567890abcdefghijklmnopqrstuvwsyz!@#$%^&*()_+}{|:'`~-=[];,./<>?~！@#￥%……&*（）——|：“《》？·【】、；‘，。/没问题", 11, date(1970, 1, 1), '1234567890abcdefghijklmnopqrstuvwsyz!@#$%^&*()_+}{|:`~-=[];,./<>?~！@#￥%……&*（）——|：“《》？·【】、；‘，。/没问题'.encode('utf-8'), "1234567890abcdefghijklmnopqrstuvwsyz!@#$%^&*()_+}{|:`~-=[];,./<>?~！@#￥%……&*（）——|：“《》？·【】、；‘，。/没问题"],
                            [True, -10, -11, 12.12345, 12.12345678901234, "test01", 11, date(1970, 1, 1), 'Hello, World!'.encode('utf-8'), "string01"],
                            [True, -0, -0, -0.0, -0.0, "test01", 11, date(1970, 1, 1), 'Hello, World!'.encode('utf-8'), "string01"],
                            [True, 10, 11, 1.1, 10011.1, "test01", 11, date(1970, 1, 1), 'Hello, World!'.encode('utf-8'), "string01"]])
    # Calculate the number of rows in the actual time series
    actual = query("select * from root.test.g3.d1")
    actual = actual + query("select * from root.test.g3.d2")
    actual = actual + query("select * from root.test.g4.d1")
    # Determine whether it meets expectations
    assert expect == actual

# Test inserting multiple Record data belonging to the same non aligned device
@pytest.mark.usefixtures('fixture_')
def test_insert_records_of_one_device():
    global session
    expect = 9
    session.insert_records_of_one_device("root.test.g1.d1", 
                                        [1, 2, 3, 4, 5, 6, 7, 8, 9], 
                                        [["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB", "STRING"],
                                         ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB", "STRING"],
                                         ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB", "STRING"],
                                         ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB", "STRING"],
                                         ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB", "STRING"],
                                         ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB", "STRING"],
                                         ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB", "STRING"],
                                         ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB", "STRING"],
                                         ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB", "STRING"]], 
                                        [[TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE, TSDataType.TEXT, TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB, TSDataType.STRING], 
                                         [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE, TSDataType.TEXT, TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB, TSDataType.STRING], 
                                         [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE, TSDataType.TEXT, TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB, TSDataType.STRING],
                                         [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE, TSDataType.TEXT, TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB, TSDataType.STRING],
                                         [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE, TSDataType.TEXT, TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB, TSDataType.STRING],
                                         [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE, TSDataType.TEXT, TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB, TSDataType.STRING],
                                         [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE, TSDataType.TEXT, TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB, TSDataType.STRING],
                                         [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE, TSDataType.TEXT, TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB, TSDataType.STRING],
                                         [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE, TSDataType.TEXT, TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB, TSDataType.STRING]], 
                                        [[False, 0, 0, 0.0, 0.0, "1234567890", 0, date(1970, 1, 1), '1234567890'.encode('utf-8'), "1234567890"],
                                         [True, -2147483648, -9223372036854775808, -0.12345678, -0.12345678901234567, "abcdefghijklmnopqrstuvwsyz", -9223372036854775808, date(1000, 1, 1), 'abcdefghijklmnopqrstuvwsyz'.encode('utf-8'), "abcdefghijklmnopqrstuvwsyz"],
                                         [True, 2147483647, 9223372036854775807, 0.123456789, 0.12345678901234567, "!@#$%^&*()_+}{|:'`~-=[];,./<>?~", 9223372036854775807, date(9999, 12, 31), '!@#$%^&*()_+}{|:`~-=[];,./<>?~'.encode('utf-8'), "!@#$%^&*()_+}{|:`~-=[];,./<>?~"],
                                         [True, 1, 1, 1.0, 1.0, "没问题", 1, date(1970, 1, 1), '没问题'.encode('utf-8'), "没问题"],
                                         [True, -1, -1, 1.1234567, 1.1234567890123456, "！@#￥%……&*（）——|：“《》？·【】、；‘，。/", 11, date(1970, 1, 1), '！@#￥%……&*（）——|：“《》？·【】、；‘，。/'.encode('utf-8'), "！@#￥%……&*（）——|：“《》？·【】、；‘，。/"],
                                         [True, 10, 11, 4.123456, 4.123456789012345, "1234567890abcdefghijklmnopqrstuvwsyz!@#$%^&*()_+}{|:'`~-=[];,./<>?~！@#￥%……&*（）——|：“《》？·【】、；‘，。/没问题", 11, date(1970, 1, 1), '1234567890abcdefghijklmnopqrstuvwsyz!@#$%^&*()_+}{|:`~-=[];,./<>?~！@#￥%……&*（）——|：“《》？·【】、；‘，。/没问题'.encode('utf-8'), "1234567890abcdefghijklmnopqrstuvwsyz!@#$%^&*()_+}{|:`~-=[];,./<>?~！@#￥%……&*（）——|：“《》？·【】、；‘，。/没问题"],
                                         [True, -10, -11, 12.12345, 12.12345678901234, "test01", 11, date(1970, 1, 1), 'Hello, World!'.encode('utf-8'), "string01"],
                                         [True, -0, -0, -0.0, -0.0, "test01", 11, date(1970, 1, 1), 'Hello, World!'.encode('utf-8'), "string01"],
                                         [True, 10, 11, 1.1, 10011.1, "test01", 11, date(1970, 1, 1), 'Hello, World!'.encode('utf-8'), "string01"]])
    # Calculate the number of rows in the actual time series
    actual = query("select * from root.test.g1.d1")
    # Determine whether it meets expectations
    assert expect == actual

# Test inserting multiple Record data belonging to the same aligned device
@pytest.mark.usefixtures('fixture_')
def test_insert_aligned_records_of_one_device():
    global session
    expect = 9
    session.insert_aligned_records_of_one_device("root.test.g3.d1", 
                                        [1, 2, 3, 4, 5, 6, 7, 8, 9], 
                                        [["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB", "STRING"],
                                         ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB", "STRING"],
                                         ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB", "STRING"],
                                         ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB", "STRING"],
                                         ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB", "STRING"],
                                         ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB", "STRING"],
                                         ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB", "STRING"],
                                         ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB", "STRING"],
                                         ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB", "STRING"]], 
                                        [[TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE, TSDataType.TEXT, TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB, TSDataType.STRING], 
                                         [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE, TSDataType.TEXT, TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB, TSDataType.STRING], 
                                         [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE, TSDataType.TEXT, TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB, TSDataType.STRING],
                                         [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE, TSDataType.TEXT, TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB, TSDataType.STRING],
                                         [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE, TSDataType.TEXT, TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB, TSDataType.STRING],
                                         [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE, TSDataType.TEXT, TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB, TSDataType.STRING],
                                         [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE, TSDataType.TEXT, TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB, TSDataType.STRING],
                                         [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE, TSDataType.TEXT, TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB, TSDataType.STRING],
                                         [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE, TSDataType.TEXT, TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB, TSDataType.STRING]], 
                                        [[False, 0, 0, 0.0, 0.0, "1234567890", 0, date(1970, 1, 1), '1234567890'.encode('utf-8'), "1234567890"],
                                         [True, -2147483648, -9223372036854775808, -0.12345678, -0.12345678901234567, "abcdefghijklmnopqrstuvwsyz", -9223372036854775808, date(1000, 1, 1), 'abcdefghijklmnopqrstuvwsyz'.encode('utf-8'), "abcdefghijklmnopqrstuvwsyz"],
                                         [True, 2147483647, 9223372036854775807, 0.123456789, 0.12345678901234567, "!@#$%^&*()_+}{|:'`~-=[];,./<>?~", 9223372036854775807, date(9999, 12, 31), '!@#$%^&*()_+}{|:`~-=[];,./<>?~'.encode('utf-8'), "!@#$%^&*()_+}{|:`~-=[];,./<>?~"],
                                         [True, 1, 1, 1.0, 1.0, "没问题", 1, date(1970, 1, 1), '没问题'.encode('utf-8'), "没问题"],
                                         [True, -1, -1, 1.1234567, 1.1234567890123456, "！@#￥%……&*（）——|：“《》？·【】、；‘，。/", 11, date(1970, 1, 1), '！@#￥%……&*（）——|：“《》？·【】、；‘，。/'.encode('utf-8'), "！@#￥%……&*（）——|：“《》？·【】、；‘，。/"],
                                         [True, 10, 11, 4.123456, 4.123456789012345, "1234567890abcdefghijklmnopqrstuvwsyz!@#$%^&*()_+}{|:'`~-=[];,./<>?~！@#￥%……&*（）——|：“《》？·【】、；‘，。/没问题", 11, date(1970, 1, 1), '1234567890abcdefghijklmnopqrstuvwsyz!@#$%^&*()_+}{|:`~-=[];,./<>?~！@#￥%……&*（）——|：“《》？·【】、；‘，。/没问题'.encode('utf-8'), "1234567890abcdefghijklmnopqrstuvwsyz!@#$%^&*()_+}{|:`~-=[];,./<>?~！@#￥%……&*（）——|：“《》？·【】、；‘，。/没问题"],
                                         [True, -10, -11, 12.12345, 12.12345678901234, "test01", 11, date(1970, 1, 1), 'Hello, World!'.encode('utf-8'), "string01"],
                                         [True, -0, -0, -0.0, -0.0, "test01", 11, date(1970, 1, 1), 'Hello, World!'.encode('utf-8'), "string01"],
                                         [True, 10, 11, 1.1, 10011.1, "test01", 11, date(1970, 1, 1), 'Hello, World!'.encode('utf-8'), "string01"]])
    # Calculate the number of rows in the actual time series
    actual = query("select * from root.test.g3.d1")
    # Determine whether it meets expectations
    assert expect == actual

# Test writes with type inference
@pytest.mark.usefixtures('fixture_')
def test_insert_str_record():
    global session
    expect = 1
    session.insert_str_record("root.test.g5.d1", 
                              1,
                              ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB", "STRING"], 
                              ["true", "10", "11", "11.11", "10011.1", "test01", "11", "1970-01-01", 'b"x12x34"', "string01"])
    # Calculate the number of rows in the actual time series
    actual = query("select * from root.test.g5.d1")
    # Determine whether it meets expectations
    assert expect == actual

# Test automatic create
@pytest.mark.usefixtures('fixture_')
def test_insert_auto_create():
    global session
    # Test non aligned tablet writing
    num = 0
    for i in range(1, 10):
        device_id = "root.repeat.g1.fd_a" + str(num)
        num = num + 1
        measurements_ = ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB", "STRING"]
        data_types_ = [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE, TSDataType.TEXT, TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB, TSDataType.STRING]
        values_ = [
            [False, 0, 0, 0.0, 0.0, "1234567890", 0, date(1970, 1, 1), '1234567890'.encode('utf-8'), "1234567890"],
            [True, -2147483648, -9223372036854775808, -0.12345678, -0.12345678901234567, "abcdefghijklmnopqrstuvwsyz", -9223372036854775808, date(1000, 1, 1), 'abcdefghijklmnopqrstuvwsyz'.encode('utf-8'), "abcdefghijklmnopqrstuvwsyz"],
            [True, 2147483647, 9223372036854775807, 0.123456789, 0.12345678901234567, "!@#$%^&*()_+}{|:'`~-=[];,./<>?~", 9223372036854775807, date(9999, 12, 31), '!@#$%^&*()_+}{|:`~-=[];,./<>?~'.encode('utf-8'), "!@#$%^&*()_+}{|:`~-=[];,./<>?~"],
            [True, 1, 1, 1.0, 1.0, "没问题", 1, date(1970, 1, 1), '没问题'.encode('utf-8'), "没问题"],
            [True, -1, -1, 1.1234567, 1.1234567890123456, "！@#￥%……&*（）——|：“《》？·【】、；‘，。/", 11, date(1970, 1, 1), '！@#￥%……&*（）——|：“《》？·【】、；‘，。/'.encode('utf-8'), "！@#￥%……&*（）——|：“《》？·【】、；‘，。/"],
            [True, 10, 11, 4.123456, 4.123456789012345, "1234567890abcdefghijklmnopqrstuvwsyz!@#$%^&*()_+}{|:'`~-=[];,./<>?~！@#￥%……&*（）——|：“《》？·【】、；‘，。/没问题", 11, date(1970, 1, 1), '1234567890abcdefghijklmnopqrstuvwsyz!@#$%^&*()_+}{|:`~-=[];,./<>?~！@#￥%……&*（）——|：“《》？·【】、；‘，。/没问题'.encode('utf-8'), "1234567890abcdefghijklmnopqrstuvwsyz!@#$%^&*()_+}{|:`~-=[];,./<>?~！@#￥%……&*（）——|：“《》？·【】、；‘，。/没问题"],
            [True, -10, -11, 12.12345, 12.12345678901234, "test01", 11, date(1970, 1, 1), 'Hello, World!'.encode('utf-8'), "string01"],
            [None, None, None, None, None, "", None, date(1970, 1, 1), ''.encode('utf-8'), ""],
            [True, -0, -0, -0.0, -0.0, "    ", 11, date(1970, 1, 1), '    '.encode('utf-8'), "    "],
            [True, 10, 11, 1.1, 10011.1, "test01", 11, date(1970, 1, 1), 'Hello, World!'.encode('utf-8'), "string01"]
        ]
        timestamps_ = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
        tablet_ = Tablet(device_id, measurements_, data_types_, values_, timestamps_)
        session.insert_tablet(tablet_)

    # Test non aligned numpy tablet writing
    num = 0
    for i in range(1, 10):
        device_id = "root.repeat.g1.fd_b"  + str(num)
        num = num + 1
        measurements_ = ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB", "STRING"]
        data_types_ = [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE, TSDataType.TEXT, TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB, TSDataType.STRING]
        np_values_ = [
            np.array([False, True, False, True, False, True, False, True, False, True], TSDataType.BOOLEAN.np_dtype()),
            np.array([0, -2147483648, 2147483647, 1, -1, 10, -10, -0, 10, 10], TSDataType.INT32.np_dtype()),
            np.array([0, -9223372036854775808, 9223372036854775807, 1, -1, 10, -10, -0, 10, 10], TSDataType.INT64.np_dtype()),
            np.array([0.0, -0.12345678, 0.123456789, 1.0, 1.1234567, 4.123456, 12.12345, -0.0, 1.1, 1.1], TSDataType.FLOAT.np_dtype()),
            np.array([0.0, -0.12345678901234567, 0.12345678901234567, 1.0, 1.1234567890123456, 4.123456789012345, 12.12345678901234, -0.0, 1.1, 1.1], TSDataType.DOUBLE.np_dtype()),
            np.array(["1234567890", "abcdefghijklmnopqrstuvwsyz", "!@#$%^&*()_+}{|:'`~-=[];,./<>?~", "没问题", "！@#￥%……&*（）——|：“《》？·【】、；‘，。/", "1234567890abcdefghijklmnopqrstuvwsyz!@#$%^&*()_+}{|:'`~-=[];,./<>?~！@#￥%……&*（）——|：“《》？·【】、；‘，。/没问题", "", "    ", "test01", "test01"], TSDataType.TEXT.np_dtype()),
            np.array([0, -9223372036854775808, 9223372036854775807, 1, -1, 10, -10, -0, 10, 10], TSDataType.TIMESTAMP.np_dtype()),
            np.array([date(1970, 1, 1), date(1000, 1, 1), date(9999, 12, 31), date(1970, 1, 1), date(1970, 1, 1), date(1970, 1, 1), date(1970, 1, 1), date(1970, 1, 1), date(1970, 1, 1), date(1970, 1, 1)], TSDataType.DATE.np_dtype()),
            np.array(['1234567890'.encode('utf-8'), 'abcdefghijklmnopqrstuvwsyz'.encode('utf-8'), '!@#$%^&*()_+}{|:`~-=[];,./<>?~'.encode('utf-8'), '没问题'.encode('utf-8'), '！@#￥%……&*（）——|：“《》？·【】、；‘，。/'.encode('utf-8'), '1234567890abcdefghijklmnopqrstuvwsyz!@#$%^&*()_+}{|:`~-=[];,./<>?~！@#￥%……&*（）——|：“《》？·【】、；‘，。/没问题'.encode('utf-8'), ''.encode('utf-8'), '   '.encode('utf-8'), '1234567890'.encode('utf-8'), '1234567890'.encode('utf-8')], TSDataType.BLOB.np_dtype()),
            np.array(["1234567890", "abcdefghijklmnopqrstuvwsyz", "!@#$%^&*()_+}{|:'`~-=[];,./<>?~", "没问题", "！@#￥%……&*（）——|：“《》？·【】、；‘，。/", "1234567890abcdefghijklmnopqrstuvwsyz!@#$%^&*()_+}{|:'`~-=[];,./<>?~！@#￥%……&*（）——|：“《》？·【】、；‘，。/没问题", "", "    ", "test01", "test01"], TSDataType.STRING.np_dtype())
        ]
        np_timestamps_ = np.array([1, 2, 3, 4, 5, 6, 7, 8, 9, 10], TSDataType.INT64.np_dtype())
        np_tablet_ = NumpyTablet(device_id, measurements_, data_types_, np_values_, np_timestamps_)
        session.insert_tablet(np_tablet_)

    # Test aligning tablet writing
    num = 0
    for i in range(1, 10):
        device_id = "root.repeat.g1.d_c"  + str(num)
        num = num + 1
        measurements_ = ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB", "STRING"]
        data_types_ = [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE, TSDataType.TEXT, TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB, TSDataType.STRING]
        values_ = [
            [False, 0, 0, 0.0, 0.0, "1234567890", 0, date(1970, 1, 1), '1234567890'.encode('utf-8'), "1234567890"],
            [True, -2147483648, -9223372036854775808, -0.12345678, -0.12345678901234567, "abcdefghijklmnopqrstuvwsyz", -9223372036854775808, date(1000, 1, 1), 'abcdefghijklmnopqrstuvwsyz'.encode('utf-8'), "abcdefghijklmnopqrstuvwsyz"],
            [True, 2147483647, 9223372036854775807, 0.123456789, 0.12345678901234567, "!@#$%^&*()_+}{|:'`~-=[];,./<>?~", 9223372036854775807, date(9999, 12, 31), '!@#$%^&*()_+}{|:`~-=[];,./<>?~'.encode('utf-8'), "!@#$%^&*()_+}{|:`~-=[];,./<>?~"],
            [True, 1, 1, 1.0, 1.0, "没问题", 1, date(1970, 1, 1), '没问题'.encode('utf-8'), "没问题"],
            [True, -1, -1, 1.1234567, 1.1234567890123456, "！@#￥%……&*（）——|：“《》？·【】、；‘，。/", 11, date(1970, 1, 1), '！@#￥%……&*（）——|：“《》？·【】、；‘，。/'.encode('utf-8'), "！@#￥%……&*（）——|：“《》？·【】、；‘，。/"],
            [True, 10, 11, 4.123456, 4.123456789012345, "1234567890abcdefghijklmnopqrstuvwsyz!@#$%^&*()_+}{|:'`~-=[];,./<>?~！@#￥%……&*（）——|：“《》？·【】、；‘，。/没问题", 11, date(1970, 1, 1), '1234567890abcdefghijklmnopqrstuvwsyz!@#$%^&*()_+}{|:`~-=[];,./<>?~！@#￥%……&*（）——|：“《》？·【】、；‘，。/没问题'.encode('utf-8'), "1234567890abcdefghijklmnopqrstuvwsyz!@#$%^&*()_+}{|:`~-=[];,./<>?~！@#￥%……&*（）——|：“《》？·【】、；‘，。/没问题"],
            [True, -10, -11, 12.12345, 12.12345678901234, "test01", 11, date(1970, 1, 1), 'Hello, World!'.encode('utf-8'), "string01"],
            [None, None, None, None, None, "", None, date(1970, 1, 1), ''.encode('utf-8'), ""],
            [True, -0, -0, -0.0, -0.0, "    ", 11, date(1970, 1, 1), '    '.encode('utf-8'), "    "],
            [True, 10, 11, 1.1, 10011.1, "test01", 11, date(1970, 1, 1), 'Hello, World!'.encode('utf-8'), "string01"]
        ]
        timestamps_ = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
        tablet_ = Tablet(device_id, measurements_, data_types_, values_, timestamps_)
        session.insert_aligned_tablet(tablet_)

    # Test alignment of numpy tablet writing
    num = 0
    for i in range(1, 10):
        device_id = "root.repeat.g1.d_d"  + str(num)
        num = num + 1
        measurements_ = ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB", "STRING"]
        data_types_ = [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE, TSDataType.TEXT, TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB, TSDataType.STRING]
        np_values_ = [
            np.array([False, True, False, True, False, True, False, True, False, True], TSDataType.BOOLEAN.np_dtype()),
            np.array([0, -2147483648, 2147483647, 1, -1, 10, -10, -0, 10, 10], TSDataType.INT32.np_dtype()),
            np.array([0, -9223372036854775808, 9223372036854775807, 1, -1, 10, -10, -0, 10, 10], TSDataType.INT64.np_dtype()),
            np.array([0.0, -0.12345678, 0.123456789, 1.0, 1.1234567, 4.123456, 12.12345, -0.0, 1.1, 1.1], TSDataType.FLOAT.np_dtype()),
            np.array([0.0, -0.12345678901234567, 0.12345678901234567, 1.0, 1.1234567890123456, 4.123456789012345, 12.12345678901234, -0.0, 1.1, 1.1], TSDataType.DOUBLE.np_dtype()),
            np.array(["1234567890", "abcdefghijklmnopqrstuvwsyz", "!@#$%^&*()_+}{|:'`~-=[];,./<>?~", "没问题", "！@#￥%……&*（）——|：“《》？·【】、；‘，。/", "1234567890abcdefghijklmnopqrstuvwsyz!@#$%^&*()_+}{|:'`~-=[];,./<>?~！@#￥%……&*（）——|：“《》？·【】、；‘，。/没问题", "", "    ", "test01", "test01"], TSDataType.TEXT.np_dtype()),
            np.array([0, -9223372036854775808, 9223372036854775807, 1, -1, 10, -10, -0, 10, 10], TSDataType.TIMESTAMP.np_dtype()),
            np.array([date(1970, 1, 1), date(1000, 1, 1), date(9999, 12, 31), date(1970, 1, 1), date(1970, 1, 1), date(1970, 1, 1), date(1970, 1, 1), date(1970, 1, 1), date(1970, 1, 1), date(1970, 1, 1)], TSDataType.DATE.np_dtype()),
            np.array(['1234567890'.encode('utf-8'), 'abcdefghijklmnopqrstuvwsyz'.encode('utf-8'), '!@#$%^&*()_+}{|:`~-=[];,./<>?~'.encode('utf-8'), '没问题'.encode('utf-8'), '！@#￥%……&*（）——|：“《》？·【】、；‘，。/'.encode('utf-8'), '1234567890abcdefghijklmnopqrstuvwsyz!@#$%^&*()_+}{|:`~-=[];,./<>?~！@#￥%……&*（）——|：“《》？·【】、；‘，。/没问题'.encode('utf-8'), ''.encode('utf-8'), '   '.encode('utf-8'), '1234567890'.encode('utf-8'), '1234567890'.encode('utf-8')], TSDataType.BLOB.np_dtype()),
            np.array(["1234567890", "abcdefghijklmnopqrstuvwsyz", "!@#$%^&*()_+}{|:'`~-=[];,./<>?~", "没问题", "！@#￥%……&*（）——|：“《》？·【】、；‘，。/", "1234567890abcdefghijklmnopqrstuvwsyz!@#$%^&*()_+}{|:'`~-=[];,./<>?~！@#￥%……&*（）——|：“《》？·【】、；‘，。/没问题", "", "    ", "test01", "test01"], TSDataType.STRING.np_dtype())
        ]
        np_timestamps_ = np.array([1, 2, 3, 4, 5, 6, 7, 8, 9, 10], TSDataType.INT64.np_dtype())
        np_tablet_ = NumpyTablet(device_id, measurements_, data_types_, np_values_, np_timestamps_)
        session.insert_aligned_tablet(np_tablet_)
        