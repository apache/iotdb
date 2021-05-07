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

## Python Native API

### Requirements

You have to install thrift (>=0.13) before using the package.



### How to use (Example)

First, download the package: `pip3 install apache-iotdb`

You can get an example of using the package to read and write data at here: [Example](https://github.com/apache/iotdb/blob/master/client-py/SessionExample.py)

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



### IoTDB Testcontainer

The Test Support is based on the lib `testcontainers` (https://testcontainers-python.readthedocs.io/en/latest/index.html) which you need to install in your project if you want to use the feature.

To start (and stop) an IoTDB Database in a Docker container simply do:
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

by default it will load the image `apache/iotdb:latest`, if you want a specific version just pass it like e.g. `IoTDBContainer("apache/iotdb:0.12.0")` to get version `0.12.0` running.



### Pandas Support

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



### Developers

#### Introduction

This is an example of how to connect to IoTDB with python, using the thrift rpc interfaces. Things are almost the same on Windows or Linux, but pay attention to the difference like path separator.



#### Prerequisites

Python3.7 or later is preferred.

You have to install Thrift (0.11.0 or later) to compile our thrift file into python code. Below is the official tutorial of installation, eventually, you should have a thrift executable.

```
http://thrift.apache.org/docs/install/
```

Before starting you need to install `requirements_dev.txt` in your python environment, e.g. by calling
```shell
pip install -r requirements_dev.txt
```



#### Compile the thrift library and Debug

In the root of IoTDB's source code folder,  run `mvn clean generate-sources -pl client-py -am`.

This will automatically delete and repopulate the folder `iotdb/thrift` with the generated thrift files.
This folder is ignored from git and should **never be pushed to git!**

**Notice** Do not upload `iotdb/thrift` to the git repo.




#### Session Client & Example

We packed up the Thrift interface in `client-py/src/iotdb/Session.py` (similar with its Java counterpart), also provided an example file `client-py/src/SessionExample.py` of how to use the session module. please read it carefully.


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



#### Tests

Please add your custom tests in `tests` folder.

To run all defined tests just type `pytest .` in the root folder.

**Notice** Some tests need docker to be started on your system as a test instance is started in a docker container using [testcontainers](https://testcontainers-python.readthedocs.io/en/latest/index.html).



#### Futher Tools

[black](https://pypi.org/project/black/) and [flake8](https://pypi.org/project/flake8/) are installed for autoformatting and linting.
Both can be run by `black .` or `flake8 .` respectively.



### Releasing

To do a release just ensure that you have the right set of generated thrift files.
Then run linting and auto-formatting.
Then, ensure that all tests work (via `pytest .`).
Then you are good to go to do a release!



#### Preparing your environment

First, install all necessary dev dependencies via `pip install -r requirements_dev.txt`.



#### Doing the Release

There is a convenient script `release.sh` to do all steps for a release.
Namely, these are

* Remove all transient directories from last release (if exists)
* (Re-)generate all generated sources via mvn
* Run Linting (flake8)
* Run Tests via pytest
* Build
* Release to pypi

