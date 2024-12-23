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

# Integration Test User Guide

Integration tests for IoTDB are in this module.

Now integration testing supports four kinds of architecture.

- `Simple`: A cluster with 1 config node and 1 data node running tree model ITs.
- `Cluster1`: A cluster with 1 config node and 3 data nodes running tree model ITs.
- `TABLE_SIMPLE`: A cluster with 1 config node and 1 data node running table model ITs.
- `TABLE_CLUSTER1`: A cluster with 1 config node and 3 data nodes running table model ITs.

## Integration Testing with Simple Consensus Mode

Integration testing in `Simple` mode can be run with both maven and IDEs like IntelliJ easily.

The maven command is:
```
mvn clean verify -DskipUTs -pl integration-test -am -P with-integration-tests 
```

Notice that, this above maven command only run IT.

And if you want to run IT in the IDE like IntelliJ, when you run the test for the first time, or when you change the code of the module that the integration test module depends on, you may need to use the following command to generate `integration-test/target/template-node` for the node to be tested.

```
mvn clean package -DskipTests -pl integration-test -am -P with-integration-tests 
```

After doing this, you can run any one just by clicking the test case and pressing `Run`, like running normal cases :).

![Simple Run](https://github.com/apache/iotdb-bin-resources/blob/main/integration-test/pic/OneCopy_Category.png?raw=true)

## Integration Testing with Cluster Mode

You can run the integration test in a 'real' cluster mode for tree model ITs. At present, we have implemented a pseudo cluster with 1 config nodes and 3 data nodes.
(As the test cases and the test environment are decoupled, we can easily implement other pseudo cluster or even a docker-based cluster later.)

The maven command is:
```
mvn clean verify -DskipUTs -pl integration-test -am -PClusterIT -P with-integration-tests 
```

You can run the integration test in `Simple` mode for table model ITs. 
The maven command is:
```
mvn clean verify -DskipUTs -pl integration-test -am -PTableSimpleIT -P with-integration-tests 
```

You can run the integration test in 'real' cluster mode for table model ITs. At present, we have implemented a pseudo cluster with 1 config nodes and 3 data nodes.
(As the test cases and the test environment are decoupled, we can easily implement other pseudo cluster or even a docker-based cluster later.)
The maven command is:
```
mvn clean verify -DskipUTs -pl integration-test -am -PTableClusterIT -P with-integration-tests 
```


If you want to run IT in `Cluster1` mode in the IDE like IntelliJ, you need to achieve the effect as the `ClusterIT` profile in maven explicitly. Follow Steps 1-4 to achieve it.


- Step 0. Optionally, when you run the test for the first time, or when you change the code of the module that the integration test module depends on, you may need to use the following command to generate `integration-test/target/template-node` for nodes of the pseudo cluster.
  
  It has the same effect as the `Simple` counterpart; these two commands' generations are the same content.
  ```
  mvn clean package -DskipTests -pl integration-test -P with-integration-tests 
  ```

- Step 1. Run(Menu) -> Edit Configurations...  
  ![Run(Menu)](https://github.com/apache/iotdb-bin-resources/blob/main/integration-test/pic/Run(Menu).png?raw=true)


- Step 2. Add New Configuration -> JUnit  
  ![Add New Configuration](https://github.com/apache/iotdb-bin-resources/blob/main/integration-test/pic/Add_New_Configuration.png?raw=true)


- Step 3. Input some fields as the following picture  
  ![ClusterIT Category](https://github.com/apache/iotdb-bin-resources/blob/main/integration-test/pic/ClusterIT_Category.png?raw=true)

## Let IDEA recognize integration test module

- Step 1. Open the Maven tab onn the right side of the IDEA window.
- Step 2. Open `Profiles` folder
- Step 3. Select `with-integration-tests` profile

## Format code style of integration test

The maven command is:

```
mvn spotless::apply -P with-integration-tests 
```