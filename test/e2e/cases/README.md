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

# IoTDB E2E tests cases

Test cases are organized into sub-directories, each of which contains the following files:

* `run.sh`: the entry of the test case.
* `cleanup.sh`: a cleanup script to clean up resources that are created during the test.
* `res`: resources files that will be mounted into the container(s) and be used there.
* `docker-compose.yaml`: orchestrates the services used in the test process.
* `README.md` (Optional): docs or notes when running this case manually.

any other additional files are completely acceptable here, for example, when building
a case to test the JDBC SDK, the files structure may be something like:

```text
.
├── README.md
├── cleanup.sh
├── docker-compose.yaml
├── app      <------- Java application that uses JDBC SDK to communicate with IoTDB
│   ├── pom.xml
│   ├── src
│   │   ├── main
│   │   │   └── java
│   │   └── test
│   │       └── java
│   └── src
│       ├── main
│       └── test
├── res
│   └── init.sql
└── run.sh
```
