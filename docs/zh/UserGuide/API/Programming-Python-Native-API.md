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

## Python 原生接口

### 依赖

在使用 Python 原生接口包前，您需要安装 thrift (>=0.13) 依赖。

### 如何使用 （示例）

首先下载最新安装包：`pip3 install apache-iotdb`

*注意：如果您想要安装 0.13.0 版本的 Python API，不要使用 `pip install apache-iotdb==0.13.0`，请使用 `pip install apache-iotdb==0.13.0.post1` 作为替代！*

您可以从这里得到一个使用该包进行数据读写的例子：[Session Example](https://github.com/apache/iotdb/blob/master/client-py/SessionExample.py)

关于对齐时间序列读写的例子：[Aligned Timeseries Session Example](https://github.com/apache/iotdb/blob/master/client-py/SessionAlignedTimeseriesExample.py)

（您需要在文件的头部添加`import iotdb`）

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
### 基本接口说明

下面将给出 Session 对应的接口的简要介绍和对应参数：

#### 初始化

* 初始化 Session

```python
session = Session(
    ip="127.0.0.1",
    port="6667",
    user="root",
    password="root",
    fetch_size=1024,
    zone_id="UTC+8"
)
```

* 初始化可连接多节点的 Session

```python
session = Session.init_from_node_urls(
    node_urls=["127.0.0.1:6667", "127.0.0.1:6668", "127.0.0.1:6669"],
    user="root",
    password="root",
    fetch_size=1024,
    zone_id="UTC+8"
)
```

* 开启 Session，并决定是否开启 RPC 压缩

```python
session.open(enable_rpc_compression=False)
```

注意: 客户端的 RPC 压缩开启状态需和服务端一致

* 关闭 Session

```python
session.close()
```

#### 数据定义接口 DDL

##### 存储组管理

* 设置存储组

```python
session.set_storage_group(group_name)
```

* 删除单个或多个存储组

```python
session.delete_storage_group(group_name)
session.delete_storage_groups(group_name_lst)
```
##### 时间序列管理

* 创建单个或多个时间序列

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

注意：目前**暂不支持**使用传感器别名。

* 删除一个或多个时间序列

```python
session.delete_time_series(paths_list)
```

* 检测时间序列是否存在

```python
session.check_time_series_exists(path)
```

#### 数据操作接口 DML

##### 数据写入

推荐使用 insert_tablet 帮助提高写入效率

* 插入一个 Tablet，Tablet 是一个设备若干行数据块，每一行的列都相同
    * **写入效率高**
    * **支持写入空值** （0.13 版本起）

Python API 里目前有两种 Tablet 实现

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
```
* Numpy Tablet

相较于普通 Tablet，Numpy Tablet 使用 [numpy.ndarray](https://numpy.org/doc/stable/reference/generated/numpy.ndarray.html) 来记录数值型数据。
内存占用和序列化耗时会降低很多，写入效率也会有很大提升。

**注意**
1. Tablet 中的每一列时间戳和值记录为一个 ndarray
2. Numpy Tablet 只支持大端类型数据，ndarray 构建时如果不指定数据类型会使用小端，因此推荐在构建 ndarray 时指定下面例子中类型使用大端。如果不指定，IoTDB Python客户端也会进行大小端转换，不影响使用正确性。

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
    np.array(["test01", "test02", "test03", "test04"]),
]
np_timestamps_ = np.array([1, 2, 3, 4], TSDataType.INT64.np_dtype())
np_tablet_ = NumpyTablet(
  "root.sg_test_01.d_02", measurements_, data_types_, np_values_, np_timestamps_
)
session.insert_tablet(np_tablet_)
```

* 插入多个 Tablet

```python
session.insert_tablets(tablet_lst)
```

* 插入一个 Record，一个 Record 是一个设备一个时间戳下多个测点的数据。

```python
session.insert_record(device_id, timestamp, measurements_, data_types_, values_)
```

* 插入多个 Record

```python
session.insert_records(
    device_ids_, time_list_, measurements_list_, data_type_list_, values_list_
    )
```

* 插入同属于一个 device 的多个 Record

```python
session.insert_records_of_one_device(device_id, time_list, measurements_list, data_types_list, values_list)
```

##### 带有类型推断的写入

当数据均是 String 类型时，我们可以使用如下接口，根据 value 的值进行类型推断。例如：value 为 "true" ，就可以自动推断为布尔类型。value 为 "3.2" ，就可以自动推断为数值类型。服务器需要做类型推断，可能会有额外耗时，速度较无需类型推断的写入慢

```python
session.insert_str_record(device_id, timestamp, measurements, string_values)
```

##### 对齐时间序列的写入

对齐时间序列的写入使用 insert_aligned_xxx 接口，其余与上述接口类似：

* insert_aligned_record
* insert_aligned_records
* insert_aligned_records_of_one_device
* insert_aligned_tablet
* insert_aligned_tablets


#### IoTDB-SQL 接口

* 执行查询语句

```python
session.execute_query_statement(sql)
```

* 执行非查询语句

```python
session.execute_non_query_statement(sql)
```

### 对 Pandas 的支持

我们支持将查询结果轻松地转换为 [Pandas Dataframe](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html)。

SessionDataSet 有一个方法`.todf()`，它的作用是消费 SessionDataSet 中的数据，并将数据转换为 pandas dataframe。

例子：

```python
from iotdb.Session import Session

ip = "127.0.0.1"
port_ = "6667"
username_ = "root"
password_ = "root"
session = Session(ip, port_, username_, password_)
session.open(False)
result = session.execute_query_statement("SELECT ** FROM root")

# Transform to Pandas Dataset
df = result.todf()

session.close()

# Now you can work with the dataframe
df = ...
```

### IoTDB Testcontainer

Python 客户端对测试的支持是基于`testcontainers`库 (https://testcontainers-python.readthedocs.io/en/latest/index.html) 的，如果您想使用该特性，就需要将其安装到您的项目中。

要在 Docker 容器中启动（和停止）一个 IoTDB 数据库，只需这样做：

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

默认情况下，它会拉取最新的 IoTDB 镜像 `apache/iotdb:latest`进行测试，如果您想指定待测 IoTDB 的版本，您只需要将版本信息像这样声明：`IoTDBContainer("apache/iotdb:0.12.0")`，此时，您就会得到一个`0.12.0`版本的 IoTDB 实例。


### 给开发人员

#### 介绍

这是一个使用 thrift rpc 接口连接到 IoTDB 的示例。在 Windows 和 Linux 上操作几乎是一样的，但要注意路径分隔符等不同之处。

#### 依赖

首选 Python3.7 或更高版本。

必须安装 thrift（0.11.0 或更高版本）才能将 thrift 文件编译为 Python 代码。下面是官方的安装教程，最终，您应该得到一个 thrift 可执行文件。

```
http://thrift.apache.org/docs/install/
```

在开始之前，您还需要在 Python 环境中安装`requirements_dev.txt`中的其他依赖：
```shell
pip install -r requirements_dev.txt
```

#### 编译 thrift 库并调试

在 IoTDB 源代码文件夹的根目录下，运行`mvn clean generate-sources -pl client-py -am`，

这个指令将自动删除`iotdb/thrift`中的文件，并使用新生成的 thrift 文件重新填充该文件夹。

这个文件夹在 git 中会被忽略，并且**永远不应该被推到 git 中！**

**注意**不要将`iotdb/thrift`上传到 git 仓库中 ！

#### Session 客户端 & 使用示例 

我们将 thrift 接口打包到`client-py/src/iotdb/session.py `中（与 Java 版本类似），还提供了一个示例文件`client-py/src/SessionExample.py`来说明如何使用 Session 模块。请仔细阅读。

另一个简单的例子：

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

#### 测试

请在`tests`文件夹中添加自定义测试。

要运行所有的测试，只需在根目录中运行`pytest . `即可。

**注意**一些测试需要在您的系统上使用 docker，因为测试的 IoTDB 实例是使用 [testcontainers](https://testcontainers-python.readthedocs.io/en/latest/index.html) 在 docker 容器中启动的。

#### 其他工具

[black](https://pypi.org/project/black/) 和 [flake8](https://pypi.org/project/flake8/) 分别用于自动格式化和 linting。
它们可以通过 `black .` 或 `flake8 .` 分别运行。

#### 发版

要进行发版，

只需确保您生成了正确的 thrift 代码，

运行了 linting 并进行了自动格式化，

然后，确保所有测试都正常通过（通过`pytest . `），

最后，您就可以将包发布到 pypi 了。

##### 准备您的环境

首先，通过`pip install -r requirements_dev.txt`安装所有必要的开发依赖。

##### 发版

有一个脚本`release.sh`可以用来执行发版的所有步骤。

这些步骤包括：

* 删除所有临时目录（如果存在）

* （重新）通过 mvn 生成所有必须的源代码

* 运行 linting （flke8）

* 通过 pytest 运行测试

* Build

* 发布到 pypi
