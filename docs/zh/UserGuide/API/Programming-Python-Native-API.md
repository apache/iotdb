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

在使用Python原生接口包前，您需要安装 thrift (>=0.13) 依赖。



### 如何使用 (示例)

首先下载包：`pip3 install apache-iotdb`

您可以从这里得到一个使用该包进行数据读写的例子：[Example](https://github.com/apache/iotdb/blob/master/client-py/SessionExample.py)

（您需要在文件的头部添加`import iotdb`）

或者：

```python
from iotdb.Session import Session

ip = "127.0.0.1"
port_ = "6667"
username_ = 'root'
password_ = 'root'
session = Session(ip, port_, username_, password_)
session.open(False)
zone = session.get_time_zone()
session.close()
```



### IoTDB Testcontainer

Python客户端对测试的支持是基于`testcontainers`库 (https://testcontainers-python.readthedocs.io/en/latest/index.html)的，如果您想使用该特性，就需要将其安装到您的项目中。

要在Docker容器中启动（和停止）一个IoTDB数据库，只需这样做:

```python
class MyTestCase(unittest.TestCase):

    def test_something(self):
        with IoTDBContainer() as c:
            session = Session('localhost', c.get_exposed_port(6667), 'root', 'root')
            session.open(False)
            result = session.execute_query_statement("SHOW TIMESERIES")
            print(result)
            session.close()
```

默认情况下，它会拉取最新的IoTDB镜像 `apache/iotdb:latest`进行测试，如果您想指定待测IoTDB的版本，您只需要将版本信息像这样声明：`IoTDBContainer("apache/iotdb:0.12.0")`，此时，您就会得到一个`0.12.0`版本的IoTDB实例。



### 对 Pandas 的支持

我们支持将查询结果轻松地转换为[Pandas Dataframe](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html)。

SessionDataSet有一个方法`.todf()`，它的作用是消费SessionDataSet中的数据，并将数据转换为pandas dataframe。

例子：

```python
from iotdb.Session import Session

ip = "127.0.0.1"
port_ = "6667"
username_ = 'root'
password_ = 'root'
session = Session(ip, port_, username_, password_)
session.open(False)
result = session.execute_query_statement("SELECT * FROM root.*")

# Transform to Pandas Dataset
df = result.todf()

session.close()

# Now you can work with the dataframe
df = ...
```



### 给开发人员

#### 介绍

这是一个使用thrift rpc接口连接到IoTDB的示例。在Windows和Linux上操作几乎是一样的，但要注意路径分隔符等不同之处。



#### 依赖

首选Python3.7或更高版本。

必须安装thrift（0.11.0或更高版本）才能将thrift文件编译为Python代码。下面是官方的安装教程，最终，您应该得到一个thrift可执行文件。

```
http://thrift.apache.org/docs/install/
```

在开始之前，您还需要在Python环境中安装`requirements_dev.txt`中的其他依赖：
```shell
pip install -r requirements_dev.txt
```



#### 编译thrift库并调试

在IoTDB源代码文件夹的根目录下，运行`mvn clean generate-sources -pl client-py -am`，

这个指令将自动删除`iotdb/thrift`中的文件，并使用新生成的thrift文件重新填充该文件夹。

这个文件夹在git中会被忽略，并且**永远不应该被推到git中！**

**注意**不要将`iotdb/thrift`上传到git仓库中 ！




#### Session 客户端 & 使用示例 

我们将thrift接口打包到`client-py/src/iotdb/session.py `中（与Java版本类似），还提供了一个示例文件`client-py/src/SessionExample.py`来说明如何使用Session模块。请仔细阅读。

另一个简单的例子：

```python
from iotdb.Session import Session

ip = "127.0.0.1"
port_ = "6667"
username_ = 'root'
password_ = 'root'
session = Session(ip, port_, username_, password_)
session.open(False)
zone = session.get_time_zone()
session.close()
```



#### 测试

请在`tests`文件夹中添加自定义测试。

要运行所有的测试，只需在根目录中运行`pytest . `即可。

**注意**一些测试需要在您的系统上使用docker，因为测试的IoTDB实例是使用[testcontainers](https://testcontainers-python.readthedocs.io/en/latest/index.html)在docker容器中启动的。



#### 其他工具

[black](https://pypi.org/project/black/) 和 [flake8](https://pypi.org/project/flake8/) 分别用于自动格式化和 linting。
它们可以通过 `black .` 或 `flake8 .` 分别运行。



### 发版

要进行发版，

只需确保您生成了正确的thrift代码，

运行了linting并进行了自动格式化，

然后，确保所有测试都正常通过（通过`pytest . `），

最后，您就可以将包发布到pypi了。



#### 准备您的环境

首先，通过`pip install -r requirements_dev.txt`安装所有必要的开发依赖。



#### 发版

有一个脚本`release.sh`可以用来执行发版的所有步骤。

这些步骤包括：

* 删除所有临时目录（如果存在）

* （重新）通过mvn生成所有必须的源代码

* 运行 linting （flke8）

* 通过 pytest 运行测试

* Build

* 发布到 pypi

