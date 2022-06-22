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

Integration Test For the MPP Architecture
===================

Integration test for the mpp architecture are in this module.

Now integration testing supports the Cluster mode and the Local Standalone mode.

Integration Testing with Cluster Mode
-------------------

You can run the integration test in cluster mode. At present, we have implemented a pseudo cluster with 1 config nodes and 3 data nodes.
(As the test cases and the test environment are decoupled, we can easily implement other pseudo cluster or even a docker-based cluster later.)

The maven command is:
```
mvn clean verify -DskipUTs -pl integration-test -am -PClusterIT
```
Notice that, this above maven command only run IT.

-------

Run in IntelliJ in cluster mode is so easy,
- Step 0. Optionally, when you run the test for the first time, or when you change the code of the module that the integration test module depends on, you may need to use the following command to generate `integration-test/target/template-node` for nodes of the pseudo cluster.
```
mvn clean package -DskipTests -pl integration-test -am -PClusterIT
```

- Step 1. Run(Menu) -> Edit Configurations...  
  ![Run(Menu)](https://github.com/apache/iotdb-bin-resources/blob/main/integration-test/pic/Run(Menu).png?raw=true)


- Step 2. Add New Configuration -> JUnit  
  ![Add New Configuration](https://github.com/apache/iotdb-bin-resources/blob/main/integration-test/pic/Add_New_Configuration.png?raw=true)


- Step 3. Input some fields as the following picture  
  ![ClusterIT Category](https://github.com/apache/iotdb-bin-resources/blob/main/integration-test/pic/ClusterIT_Category.png?raw=true)

Integration Testing with Local Standalone Mode
-------------------

Integration testing with local standalone mode can be run with both maven and IDEs like IntelliJ.

The maven command is:
```
mvn clean verify -DskipUTs -pl integration-test -am -PLocalStandaloneOnMppIT
```

-------
And if you want to run IT in the IDE like IntelliJ, you need to achieve the effect as the `LocalStandaloneOnMppIT` profile in maven. Follow Steps 1-4 to achieve it.

- Step 0. Optionally, when you run the test for the first time, or when you change the code of the module that the integration test module depends on, you may need to use the following command to generate `integration-test/target/template-node` for the node of the local standalone.   
It has the same effect as step 0 of the cluster mode counterpart; these two command's generations are the same content.
```
mvn clean package -DskipTests -pl integration-test -am -PLocalStandaloneOnMppIT
```

- Step 1. Run(Menu) -> Edit Configurations...  
  ![Run(Menu)](https://github.com/apache/iotdb-bin-resources/blob/main/integration-test/pic/Run(Menu).png?raw=true)


- Step 2. Add New Configuration -> JUnit  
  ![Add New Configuration](https://github.com/apache/iotdb-bin-resources/blob/main/integration-test/pic/Add_New_Configuration.png?raw=true)


- Step 3. Input some fields as the following picture  
  ![StandaloneOnMppIT Category](https://github.com/apache/iotdb-bin-resources/blob/main/integration-test/pic/StandaloneOnMppIT_Category.png?raw=true)
