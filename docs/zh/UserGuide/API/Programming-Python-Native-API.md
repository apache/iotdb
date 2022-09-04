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

Python 客户端对测试的支持是基于`testcontainers`库 (https://testcontainers-python.readthedocs.io/en/latest/index.html) 的，如果您想使用该特性，就需要将其安装到您的项目中。

要在 Docker 容器中启动（和停止）一个 IoTDB 数据库，只需这样做：

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

默认情况下，它会拉取最新的 IoTDB 镜像 `apache/iotdb:latest`进行测试，如果您想指定待测 IoTDB 的版本，您只需要将版本信息像这样声明：`IoTDBContainer("apache/iotdb:0.12.0")`，此时，您就会得到一个`0.12.0`版本的 IoTDB 实例。

### 对 Pandas 的支持

我们支持将查询结果轻松地转换为 [Pandas Dataframe](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html)。

SessionDataSet 有一个方法`.todf()`，它的作用是消费 SessionDataSet 中的数据，并将数据转换为 pandas dataframe。

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

**注意**一些测试需要在您的系统上使用 docker，因为测试的 IoTDB 实例是使用 [testcontainers](https://testcontainers-python.readthedocs.io/en/latest/index.html) 在 docker 容器中启动的。

#### 其他工具

[black](https://pypi.org/project/black/) 和 [flake8](https://pypi.org/project/flake8/) 分别用于自动格式化和 linting。
它们可以通过 `black .` 或 `flake8 .` 分别运行。

### 发版

要进行发版，

只需确保您生成了正确的 thrift 代码，

运行了 linting 并进行了自动格式化，

然后，确保所有测试都正常通过（通过`pytest . `），

最后，您就可以将包发布到 pypi 了。

#### 准备您的环境

首先，通过`pip install -r requirements_dev.txt`安装所有必要的开发依赖。

#### 发版

有一个脚本`release.sh`可以用来执行发版的所有步骤。

这些步骤包括：

* 删除所有临时目录（如果存在）

* （重新）通过 mvn 生成所有必须的源代码

* 运行 linting （flke8）

* 通过 pytest 运行测试

* Build

* 发布到 pypi
