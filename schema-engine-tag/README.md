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

`TagSchemaRegion` is an implementation of `SchemaRegion`.

# How To Use

Firstly, you should package **schema-engine-rocksdb** by the following command:

```shell
mvn clean package -pl schema-engine-tag -am -DskipTests
```

After that, you can get a **conf** directory and a **lib** directory in
schema-engine-tag/target/schema-engine-tag. Copy the file in the conf directory to the conf directory of server,
and copy the files in the lib directory to the lib directory of server.

Then, open the **iotdb-datanode.properties** in the conf directory of server, and set the `schema_engine_mode` to
Tag, set the `enable_id_table` to true. Restart the IoTDB, the system will use `TagSchemaRegion` to manage
the metadata.