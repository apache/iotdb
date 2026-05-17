<!--

    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

        http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.

-->

# Apache IoTDB

[![GitHub release](https://img.shields.io/github/release/apache/iotdb.svg)](https://github.com/apache/iotdb/releases)
[![License](https://img.shields.io/badge/license-Apache%202-4EB1BA.svg)](https://www.apache.org/licenses/LICENSE-2.0.html)
![](https://github-size-badge.herokuapp.com/apache/iotdb.svg)
![](https://img.shields.io/github/downloads/apache/iotdb/total.svg)
![](https://img.shields.io/badge/platform-win%20%7C%20macos%20%7C%20linux-yellow.svg)
[![IoTDB Website](https://img.shields.io/website-up-down-green-red/https/shields.io.svg?label=iotdb-website)](https://iotdb.apache.org/)


Apache IoTDB（物联网数据库）是一款物联网原生数据库，具有高性能的数据管理和分析能力，可部署在边缘和云端。
凭借其轻量级架构、高性能和丰富的功能集，以及与 Apache Hadoop、Spark 和 Flink 的深度集成，
Apache IoTDB 能够满足物联网工业领域中海量数据存储、高速数据写入和复杂数据分析的需求。

## Python 原生 API

### 环境要求

使用本包之前，需要先安装 thrift (>=0.14.1)。



### 使用方法（示例）

首先，安装最新的包：`pip3 install apache-iotdb`

读写数据的使用示例请参考：[示例](https://github.com/apache/iotdb/blob/master/iotdb-client/client-py/SessionExample.py)

对齐时间序列的示例：[对齐时间序列 Session 示例](https://github.com/apache/iotdb/blob/master/iotdb-client/client-py/SessionAlignedTimeseriesExample.py)

（需要在文件开头添加 `import iotdb`）

或者：

```python
from iotdb.Session import Session

ip = "127.0.0.1"
port_ = "6667"
username_ = "root"
password_ = "root"
session = Session(ip, port_, username_, password_)
session.open(False)
zone = session.get_time_zone()
session.close()
```

### 初始化

* 初始化 Session

```python
session = Session(ip, port_, username_, password_, fetch_size=1024, zone_id="Asia/Shanghai")
```

* 打开 Session，可通过参数指定是否启用 RPC 压缩

```python
session.open(enable_rpc_compression=False)
```

注意：客户端的 RPC 压缩状态必须与 IoTDB 服务器一致。

* 关闭 Session

```python
session.close()
```

### 数据定义接口（DDL 接口）

#### 数据库管理

* 创建数据库

```python
session.set_storage_group(group_name)
```

* 删除一个或多个数据库

```python
session.delete_storage_group(group_name)
session.delete_storage_groups(group_name_lst)
```
#### 时间序列管理

* 创建一个或多个时间序列

```python
session.create_time_series(ts_path, data_type, encoding, compressor,
    props=None, tags=None, attributes=None, alias=None)
      
session.create_multi_time_series(
    ts_path_lst, data_type_lst, encoding_lst, compressor_lst,
    props_lst=None, tags_lst=None, attributes_lst=None, alias_lst=None
)
```

* 创建对齐时间序列

```python
session.create_aligned_time_series(
    device_id, measurements_lst, data_type_lst, encoding_lst, compressor_lst
)
```

注意：目前**不支持**度量（measurement）的别名。

* 删除一个或多个时间序列

```python
session.delete_time_series(paths_list)
```

* 检查指定的时间序列是否存在

```python
session.check_time_series_exists(path)
```

### 数据操作接口（DML 接口）

#### 写入

建议使用 insertTablet 以提高写入效率。

* 插入一个 Tablet，即一个设备的多行数据，每行具有相同的度量

    * **更好的写入性能**
    * **支持空值**：用任意值填充空值位置，然后通过 BitMap 标记空值（v0.13 起支持）


Python API 中有两种 Tablet 实现。

* 普通 Tablet

```python
values_ = [
    [False, 10, 11, 1.1, 10011.1, "test01"],
    [True, 100, 11111, 1.25, 101.0, "test02"],
    [False, 100, 1, 188.1, 688.25, "test03"],
    [True, 0, 0, 0, 6.25, "test04"],
]
timestamps_ = [1, 2, 3, 4]
tablet_ = Tablet(
    device_id, measurements_, data_types_, values_, timestamps_
)
session.insert_tablet(tablet_)

values_ = [
    [None, 10, 11, 1.1, 10011.1, "test01"],
    [True, None, 11111, 1.25, 101.0, "test02"],
    [False, 100, None, 188.1, 688.25, "test03"],
    [True, 0, 0, 0, None, None],
]
timestamps_ = [16, 17, 18, 19]
tablet_ = Tablet(
    device_id, measurements_, data_types_, values_, timestamps_
)
session.insert_tablet(tablet_)
```
* Numpy Tablet

与普通 Tablet 相比，Numpy Tablet 使用 [numpy.ndarray](https://numpy.org/doc/stable/reference/generated/numpy.ndarray.html) 来存储数据。
由于内存占用更少且序列化开销更低，写入性能更好。

**注意**
1. Tablet 中的时间列和值列均为 ndarray。
2. 建议为每个 ndarray 使用特定的 dtype，参见下方示例（使用默认 dtype 也可以）。

```python
import numpy as np
data_types_ = [
    TSDataType.BOOLEAN,
    TSDataType.INT32,
    TSDataType.INT64,
    TSDataType.FLOAT,
    TSDataType.DOUBLE,
    TSDataType.TEXT,
]
np_values_ = [
    np.array([False, True, False, True], TSDataType.BOOLEAN.np_dtype()),
    np.array([10, 100, 100, 0], TSDataType.INT32.np_dtype()),
    np.array([11, 11111, 1, 0], TSDataType.INT64.np_dtype()),
    np.array([1.1, 1.25, 188.1, 0], TSDataType.FLOAT.np_dtype()),
    np.array([10011.1, 101.0, 688.25, 6.25], TSDataType.DOUBLE.np_dtype()),
    np.array(["test01", "test02", "test03", "test04"], TSDataType.TEXT.np_dtype()),
]
np_timestamps_ = np.array([1, 2, 3, 4], TSDataType.INT64.np_dtype())
np_tablet_ = NumpyTablet(
  device_id, measurements_, data_types_, np_values_, np_timestamps_
)
session.insert_tablet(np_tablet_)

# 插入一个包含空值的 numpy tablet
np_values_ = [
    np.array([False, True, False, True], TSDataType.BOOLEAN.np_dtype()),
    np.array([10, 100, 100, 0], TSDataType.INT32.np_dtype()),
    np.array([11, 11111, 1, 0], TSDataType.INT64.np_dtype()),
    np.array([1.1, 1.25, 188.1, 0], TSDataType.FLOAT.np_dtype()),
    np.array([10011.1, 101.0, 688.25, 6.25], TSDataType.DOUBLE.np_dtype()),
    np.array(["test01", "test02", "test03", "test04"], TSDataType.TEXT.np_dtype()),
]
np_timestamps_ = np.array([98, 99, 100, 101], TSDataType.INT64.np_dtype())
np_bitmaps_ = []
for i in range(len(measurements_)):
    np_bitmaps_.append(BitMap(len(np_timestamps_)))
np_bitmaps_[0].mark(0)
np_bitmaps_[1].mark(1)
np_bitmaps_[2].mark(2)
np_bitmaps_[4].mark(3)
np_bitmaps_[5].mark(3)
np_tablet_with_none = NumpyTablet(
    device_id, measurements_, data_types_, np_values_, np_timestamps_, np_bitmaps_
)
session.insert_tablet(np_tablet_with_none)
```

* 插入多个 Tablet

```python
session.insert_tablets(tablet_lst)
```

* 插入一条记录

```python
session.insert_record(device_id, timestamp, measurements_, data_types_, values_)
```

* 插入多条记录

```python
session.insert_records(
    device_ids_, time_list_, measurements_list_, data_type_list_, values_list_
)
```

* 插入属于同一设备的多条记录。
  提供类型信息后，服务器无需进行类型推断，可获得更好的性能。


```python
session.insert_records_of_one_device(device_id, time_list, measurements_list, data_types_list, values_list)
```

#### 带类型推断的写入

当数据为 String 类型时，可以使用以下接口根据值本身进行类型推断。例如，值为 "true" 时会自动推断为布尔类型，值为 "3.2" 时会自动推断为浮点类型。不提供类型信息时，服务器需要进行类型推断，这可能会消耗一定时间。

* 插入一条记录，包含一个设备在某个时间戳的多个度量值

```python
session.insert_str_record(device_id, timestamp, measurements, string_values)
```

#### 对齐时间序列的写入

对齐时间序列的写入使用 insert_aligned_XXX 系列接口，其余与上述接口类似：

* insert_aligned_record
* insert_aligned_records
* insert_aligned_records_of_one_device
* insert_aligned_tablet
* insert_aligned_tablets


### IoTDB-SQL 接口

* 执行查询语句

```python
session.execute_query_statement(sql)
```

* 执行非查询语句

```python
session.execute_non_query_statement(sql)
```

* 执行语句

```python
session.execute_statement(sql)
```



### Pandas 支持

为了方便地将查询结果转换为 [Pandas DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html)，
SessionDataSet 提供了 `.todf()` 方法，可以消费数据集并将其转换为 Pandas DataFrame。

示例：

```python
from iotdb.Session import Session

ip = "127.0.0.1"
port_ = "6667"
username_ = "root"
password_ = "root"
session = Session(ip, port_, username_, password_)
session.open(False)
result = session.execute_query_statement("SELECT * FROM root.*")

# 转换为 Pandas DataFrame
df = result.todf()

session.close()

# 现在可以操作 DataFrame
df = ...
```


### IoTDB 测试容器

测试支持基于 `testcontainers` 库 (https://testcontainers-python.readthedocs.io/en/latest/index.html)，如果需要使用该功能，需要在项目中安装此库。

在 Docker 容器中启动（和停止）IoTDB 数据库：
```python
class MyTestCase(unittest.TestCase):

    def test_something(self):
        with IoTDBContainer() as c:
            session = Session("localhost", c.get_exposed_port(6667), "root", "root")
            session.open(False)
            result = session.execute_query_statement("SHOW TIMESERIES")
            print(result)
            session.close()
```

默认会加载 `apache/iotdb:latest` 镜像，如果需要特定版本，可以传入版本号，如 `IoTDBContainer("apache/iotdb:0.12.0")`。


### IoTDB DBAPI

IoTDB DBAPI 实现了 Python DB API 2.0 规范 (https://peps.python.org/pep-0249/)，定义了 Python 中访问数据库的通用接口。

#### 示例
+ 初始化

初始化参数与 Session 部分一致（sqlalchemy_mode 除外）。
```python
from iotdb.dbapi import connect

ip = "127.0.0.1"
port_ = "6667"
username_ = "root"
password_ = "root"
conn = connect(ip, port_, username_, password_,fetch_size=1024,zone_id="Asia/Shanghai",sqlalchemy_mode=False)
cursor = conn.cursor()
```
+ 简单 SQL 语句执行
```python
cursor.execute("SELECT * FROM root.*")
for row in cursor.fetchall():
    print(row)
```

+ 带参数执行 SQL

IoTDB DBAPI 支持 pyformat 风格的参数
```python
cursor.execute("SELECT * FROM root.* WHERE time < %(time)s",{"time":"2017-11-01T00:08:00.000"})
for row in cursor.fetchall():
    print(row)
```

+ 批量执行 SQL
```python
seq_of_parameters = [
    {"timestamp": 1, "temperature": 1},
    {"timestamp": 2, "temperature": 2},
    {"timestamp": 3, "temperature": 3},
    {"timestamp": 4, "temperature": 4},
    {"timestamp": 5, "temperature": 5},
]
sql = "insert into root.cursor(timestamp,temperature) values(%(timestamp)s,%(temperature)s)"
cursor.executemany(sql,seq_of_parameters)
```

+ 关闭连接和游标
```python
cursor.close()
conn.close()
```

### IoTDB SQLAlchemy 方言

IoTDB SQLAlchemy 方言为 IoTDB 的**表模型**（IoTDB 2.0+）提供了标准的 SQLAlchemy 接口，
支持 DDL（CREATE/DROP TABLE）、DML（INSERT/SELECT/DELETE）和 Schema 反射。

完整可运行示例请参考：[SQLAlchemy 示例](https://github.com/apache/iotdb/blob/master/iotdb-client/client-py/sqlalchemy_example.py)

#### 环境要求

```bash
pip install apache-iotdb sqlalchemy
```

#### 连接 URL

```
iotdb://username:password@host:port/database
```

`/database` 部分是可选的。如果省略，可以通过在表上指定 `schema=` 或使用 `USE` 语句来指定数据库。

```python
from sqlalchemy import create_engine

engine = create_engine("iotdb://root:root@127.0.0.1:6667")
```

#### 元数据映射

| SQLAlchemy | IoTDB  |
|------------|--------|
| Schema     | 数据库  |
| Table      | 表     |
| Column     | 列     |

#### 列类别

IoTDB 表模型的列具有类别属性，需要通过 `iotdb_category` 方言选项指定：

| 类别         | 描述                                        |
|-------------|---------------------------------------------|
| `TIME`      | 时间戳列（未指定时自动生成）                    |
| `TAG`       | 标识/索引列（如区域、设备 ID）                  |
| `ATTRIBUTE` | 描述性列（如型号、固件版本）                    |
| `FIELD`     | 度量/指标列（如温度、湿度）                     |

#### 数据类型映射

| IoTDB     | SQLAlchemy   |
|-----------|--------------|
| BOOLEAN   | Boolean      |
| INT32     | Integer      |
| INT64     | BigInteger   |
| FLOAT     | Float        |
| DOUBLE    | Float        |
| STRING    | String       |
| TEXT      | Text         |
| BLOB      | LargeBinary  |
| TIMESTAMP | DateTime     |
| DATE      | Date         |

#### DDL — 创建表

在每个列上使用 `iotdb_category`，在表上使用 `iotdb_ttl`：

```python
from sqlalchemy import Table, Column, Float, String, Boolean, MetaData

metadata = MetaData()
sensors = Table(
    "sensors",
    metadata,
    Column("region", String, iotdb_category="TAG"),
    Column("device_id", String, iotdb_category="TAG"),
    Column("model", String, iotdb_category="ATTRIBUTE"),
    Column("temperature", Float, iotdb_category="FIELD"),
    Column("humidity", Float, iotdb_category="FIELD"),
    Column("status", Boolean, iotdb_category="FIELD"),
    schema="my_database",
    iotdb_ttl=86400000,  # TTL 单位为毫秒（1 天）
)

metadata.create_all(engine)
```

如需显式定义 TIME 列而非使用自动生成的时间列：

```python
from sqlalchemy import BigInteger

events = Table(
    "events",
    metadata,
    Column("ts", BigInteger, iotdb_category="TIME"),
    Column("device_id", String, iotdb_category="TAG"),
    Column("value", Float, iotdb_category="FIELD"),
    schema="my_database",
)
```

#### DML — 插入、查询、删除

```python
with engine.connect() as conn:
    # 插入
    conn.execute(
        sensors.insert().values(
            region="asia", device_id="d001", temperature=25.5, humidity=60.0, status=True,
        )
    )

    # 查询全部
    result = conn.execute(sensors.select())
    for row in result:
        print(row)

    # 带 WHERE、ORDER BY、LIMIT 的查询
    result = conn.execute(
        sensors.select()
        .where(sensors.c.region == "asia")
        .order_by(sensors.c.temperature)
        .limit(10)
    )

    # 删除
    conn.execute(sensors.delete().where(sensors.c.device_id == "d001"))
```

#### Schema 反射

```python
from sqlalchemy import inspect

insp = inspect(engine)

# 列出所有数据库
schemas = insp.get_schema_names()

# 列出数据库中的表
tables = insp.get_table_names(schema="my_database")

# 获取列详情
columns = insp.get_columns(table_name="sensors", schema="my_database")
for col in columns:
    print(col["name"], col["type"], col.get("iotdb_category"))
```

#### 原生 SQL

```python
from sqlalchemy.sql import text

with engine.connect() as conn:
    result = conn.execute(text("SELECT * FROM my_database.sensors"))
    for row in result:
        print(row)
```


## 开发者指南

### 简介

这是一个使用 Thrift RPC 接口连接 IoTDB 的 Python 示例。Windows 和 Linux 上的操作基本相同，但请注意路径分隔符等差异。



### 前提条件

推荐使用 Python 3.6 或更高版本。

需要安装 Thrift（0.14.1 或更高版本）来将 Thrift 文件编译为 Python 代码。以下是官方安装教程，最终你需要有一个 thrift 可执行文件。

```
http://thrift.apache.org/docs/install/
```

开始之前，需要在 Python 环境中安装 `requirements_dev.txt`，例如：
```shell
pip install -r requirements_dev.txt
```



### 编译 Thrift 库和调试

在 IoTDB 源代码根目录下，运行 `mvn clean generate-sources -pl client-py -am`。

这将自动删除并重新生成 `iotdb/thrift` 目录中的 Thrift 文件。
该目录已被 git 忽略，**不应推送到 git！**

**注意** 不要将 `iotdb/thrift` 上传到 git 仓库。




### Session 客户端和示例

我们将 Thrift 接口封装在 `client-py/src/iotdb/Session.py` 中（类似于 Java 版本），并提供了示例文件 `client-py/src/SessionExample.py`，展示如何使用 Session 模块，请仔细阅读。


或者，一个简单的示例：

```python
from iotdb.Session import Session

ip = "127.0.0.1"
port_ = "6667"
username_ = "root"
password_ = "root"
session = Session(ip, port_, username_, password_)
session.open(False)
zone = session.get_time_zone()
session.close()
```



### 测试

请将自定义测试添加到 `tests` 目录中。

在根目录下运行 `pytest .` 即可执行所有已定义的测试。

**注意** 部分测试需要系统上运行 Docker，因为会使用 [testcontainers](https://testcontainers-python.readthedocs.io/en/latest/index.html) 在 Docker 容器中启动测试实例。



### 其他工具

[black](https://pypi.org/project/black/) 和 [flake8](https://pypi.org/project/flake8/) 已安装，分别用于自动格式化和代码检查。
可以分别通过 `black .` 或 `flake8 .` 运行。



## 发布

发布前，请确保拥有正确的 Thrift 生成文件。
然后运行代码检查和自动格式化。
确保所有测试通过（通过 `pytest .`）。
然后即可进行发布！



### 准备环境

首先，通过 `pip install -r requirements_dev.txt` 安装所有必要的开发依赖。



### 执行发布

提供了一个便捷脚本 `release.sh` 来执行发布的所有步骤，包括：

* 删除上次发布的临时目录（如果存在）
* 通过 mvn（重新）生成所有源文件
* 通过 pytest 运行测试（可选）
* 构建
* 发布到 PyPI
