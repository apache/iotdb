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
# Building C++ Client

To compile cpp client, add "-P client-cpp" option to maven build command.

The compiling requires the module "compile-tools" to be built first.
For more information, please refer to "compile-tools/README.md".


## Compile and Test:

`mvn integration-test -P client-cpp -pl client-cpp,server -am -Diotdb.test.skip=true -Dtsfile.test.skip=true -Djdbc.test.skip=true`

