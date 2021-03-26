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

[![Build Status](https://www.travis-ci.org/apache/iotdb.svg?branch=master)](https://www.travis-ci.org/apache/iotdb)
[![codecov](https://codecov.io/gh/thulab/iotdb/branch/master/graph/badge.svg)](https://codecov.io/gh/thulab/iotdb)
[![GitHub release](https://img.shields.io/github/release/apache/iotdb.svg)](https://github.com/apache/iotdb/releases)
[![License](https://img.shields.io/badge/license-Apache%202-4EB1BA.svg)](https://www.apache.org/licenses/LICENSE-2.0.html)
![](https://github-size-badge.herokuapp.com/apache/iotdb.svg)
![](https://img.shields.io/github/downloads/apache/iotdb/total.svg)
![](https://img.shields.io/badge/platform-win10%20%7C%20macox%20%7C%20linux-yellow.svg)
![](https://img.shields.io/badge/java--language-1.8-blue.svg)
[![IoTDB Website](https://img.shields.io/website-up-down-green-red/https/shields.io.svg?label=iotdb-website)](https://iotdb.apache.org/)


Apache IoTDB (Database for Internet of Things) is an IoT native database with high performance for 
data management and analysis, deployable on the edge and the cloud. Due to its light-weight 
architecture, high performance and rich feature set together with its deep integration with 
Apache Hadoop, Spark and Flink, Apache IoTDB can meet the requirements of massive data storage, 
high-speed data ingestion and complex data analysis in the IoT industrial fields.


# Apache IoTDB Python Client API

Using the package, you can write data to IoTDB, read data from IoTDB and maintain the schema of IoTDB.

## Requirements

You have to install thrift (>=0.13) before using the package.

## How to use (Example)

First, download the package: `pip3 install apache-iotdb`

You can get an example of using the package to read and write data at here: [Example](https://github.com/apache/iotdb/blob/rel/0.11/client-py/src/SessionExample.py)

(you need to add `import iotdb` in the head of the file)

Or:

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

## IoTDB Testcontainer

The Test Support is based on the lib `testcontainers` (https://testcontainers-python.readthedocs.io/en/latest/index.html) which you need to install in your project if you want to use the feature.

To start (and stop) an IoTDB Database in a Docker container simply do:
```
class MyTestCase(unittest.TestCase):

    def test_something(self):
        with IoTDBContainer() as c:
            session = Session('localhost', c.get_exposed_port(6667), 'root', 'root')
            session.open(False)
            result = session.execute_query_statement("SHOW TIMESERIES")
            print(result)
            session.close()
```

by default it will load the image `apache/iotdb:latest`, if you want a specific version just pass it like e.g. `IoTDBContainer("apache/iotdb:0.10.0")` to get version `0.10.0` running.

## Pandas Support

To easily transform a query result to a [Pandas Dataframe](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html)
the SessionDataSet has a method `.todf()` which consumes the dataset and transforms it to a pandas dataframe.

Example:

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

## Developers

### Introduction

This is an example of how to connect to IoTDB with python, using the thrift rpc interfaces. Things
are almost the same on Windows or Linux, but pay attention to the difference like path separator.

### Prerequisites

python3.7 or later is preferred.

You have to install Thrift (0.11.0 or later) to compile our thrift file into python code. Below is the official
tutorial of installation, eventually, you should have a thrift executable.

```
http://thrift.apache.org/docs/install/
```

### Compile the thrift library and Debug

In the root of IoTDB's source code folder,  run `mvn generate-sources -pl client-py -am`.

Then a complete project will be generated at `client-py/target/pypi` folder.
But !BE CAUTIOUS!
All your modifications in `client-py/target/pypi` must be copied manually to `client-py/src/` folder.
Otherwise once you run `mvn clean`, you will lose all your effort.

Or, you can also copy `client-py/target/pypi/iotdb/thrift` folder to `client-py/src/thrift`, then the
`src` folder will become also a complete python project.
But !BE CAUTIOUS!
Do not upload `client-py/src/thrift` to the git repo.


### Session Client & Example

We packed up the Thrift interface in `client-py/src/iotdb/Session.py` (similar with its Java counterpart), also provided
an example file `client-py/src/SessionExample.py` of how to use the session module. please read it carefully.


Or, another simple example:

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

### test file

You can use `client-py/src/SessionTest.py` to test python session, if the test has been passed, it will return 0. Otherwise it will return 1. You can use the printed message to locate failed operations and the reason of them.

Notice: you should start IoTDB server firstly and then run the test.
