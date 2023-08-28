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

# Apache IoTDB MLNode

For developers, you can start an ML Node through the following steps.

- Step 1: build project

```shell
mvn clean package -DskipUTs -pl mlnode -am
```

```shell
cd mlnode
poetry build
```

- Step 2: install

```shell
pip install dist/apache_iotdb_mlnode-1.0.0-py3-none-any.whl --force-reinstall
```

- Step 3: start node

```shell
mlnode start
```