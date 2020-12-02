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

# Python Client
## Introduction
This is an example of how to connect to IoTDB with python, using the thrift rpc interfaces. Things 
are almost the same on Windows or Linux, but pay attention to the difference like path separator.

## Prerequisites
python3.7 or later is preferred.

You have to install Thrift (0.11.0 or later) to compile our thrift file into python code. Below is the official
tutorial of installation, eventually, you should have a thrift executable.
```
http://thrift.apache.org/docs/install/
```

## Compile the thrift library and Debug

In the root of IoTDB's source code folder,  run `mvn generate-sources -pl client-py -am`.

Then a complete project will be generated at `client-py/target/pypi` folder. 
But !BE CAUTIOUS!
All your modifications in `client-py/target/pypi` must be copied manually to `client-py/src/` folder.
Otherwise once you run `mvn clean`, you will lose all your effort.

Or, you can also copy `client-py/target/pypi/iotdb/thrift` folder to `client-py/src/thrift`, then the 
`src` folder will become also a complete python project. 
But !BE CAUTIOUS!
Do not upload `client-py/src/thrift` to the git repo.


## Session Client & Example
We packed up the Thrift interface in `client-py/src/iotdb/Session.py` (similar with its Java counterpart), also provided 
an example file `client-py/src/SessionExample.py` of how to use the session module. please read it carefully.


Or, another simple example:

```$python

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