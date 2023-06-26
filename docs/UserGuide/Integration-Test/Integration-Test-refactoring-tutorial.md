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

# Developer Document for Integration Test

**Integration test** is one of the phases in software testing, when different software modules are put together  and tested as a whole. Integration tests are for evaluating whether a system or component meets the target functional requirements.


## Apache IoTDB Integration Test Criteria 

### The Environment of the integration test of Apache IoTDB


There are three kinds of environments for Apache IoTDB integration test, correspondingly **local standalone, Cluster, and remote.**   The integration test should be conducted on at least one of them. Details of the three kinds are as follows.

1. Local standalone. It is set up for integration testing of IoTDB, the standalone version. Any change of the configurations of IoTDB would require updating the configuration files before starting the database. 
2. Cluster. It is set up for integration testing of IoTDB, the distribution version (pseudo-distribution). Any change of the configurations of IoTDB would require updating the configuration files before starting the database. 
3. Remote. It is set up for the integration testing of a remote IoTDB instance, which could be either a standalone instance or a node in a remote cluster. Any change of the configuration is restricted and is not allowed currently. 

Integration test developers need to specify at least one of the environments when writing the tests. Please check the details below.


### Black-Box Testing

Black-box testing is a software testing method that evaluates the functionality of a program without regard to its internal structure or how it works. Developers do not need to understand the internal logic of the application for testing. **Apache IoTDB integration tests are conducted as black-box tests. Any test interacting with the system through JDBC or Session API is considered a black-box test case.** Moreover, the validation of the output should also be implemented through the JDBC or Session API.


### Steps of an integration test

Generally, there are three steps to finish the integration test, (1) constructing the test class and annotating the environment, (2) housekeeping to prepare for the test and clean up the environment after the test, and (3) implementing the logic of the integration test. To test IoTDB not under the default configuration, the configuration should be changed before the test, which will be introduced in section 4. 

<img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://github.com/apache/iotdb-bin-resources/blob/main/integration-test/pic/step_en.svg">

#### 1. Integration Test Class (IT Class) and Annotations

When writing new IT classes, the developers are encouraged to create the new ones in the [integration-test](https://github.com/apache/iotdb/tree/master/integration-test) module. Except for the classes serving the other test cases, the classes containing integration tests to evaluate the functionality of IoTDB should be named "function"+"IT". For example, the test for auto-registration metadata in IoTDB is named “<span style="color:green">IoTDBAutoCreateSchema</span><span style="color:red">IT</span>”. 

- Category`` Annotation. **When creating new IT classes, the ```@Category``` should be introduced explicitly**, and the test environment should be specified by ```LocalStandaloneIT.class```, ```ClusterIT.class```, and ```RemoteIT.class```, which corresponds to the Local Standalone, Cluster and Remote environment respectively. **In general, ```LocalStandaloneIT.class``` and ```ClusterIT.class``` should both be included**.  Only in the case when some functionalities are only supported in the standalone version can we include   ```LocalStandaloneIT.class``` solely. 
- RunWith Annotation. The ```@RunWith(IoTDBTestRunner.class)```  annotation should be included in every IT class. 


```java
// Introduce annotations to IoTDBAliasIT.class. The environments include local standalone, cluster and remote. 
@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class, ClusterIT.class, RemoteIT.class})
public class IoTDBAliasIT {
  ...
}

// Introduce annotations to IoTDBAlignByDeviceIT.class. The environments include local standalone and cluster. 
@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class, ClusterIT.class})
public class IoTDBAlignByDeviceIT {
  ...
}
```

#### 2. Housekeeping to Prepare for the Test and Clean up the Environment after the Test

Preparations before the test include starting an IoTDB (single or cluster) instance and preparing data for the test. The logic should be  implemented in the ```setUp()``` method, and the method should follow the annotation ```@BeforeClass``` or ```@Before```. 
The former means that this method is the first method executed for the IT class and is executed only once. The latter indicates that ```setUp()``` will be executed before each test method in the IT class. 

- Please start IoTDB instance through the factor class, i.e., ```EnvFactory.getEnv().initBeforeClass()```.
- Data preparation for the test includes registering databases, registering time series, and writing time series data as required by the test. It is recommended to implement a separate method within the IT class to prepare the data, such as ```insertData()```. 
Please try to take advantage of the ```executeBatch()``` in JDBC or ```insertRecords()``` and ```insertTablets()``` in Session API if multiple statements or operations are to be executed. 

```java
@BeforeClass
public static void setUp() throws Exception {
  // start an IoTDB instance
  EnvFactory.getEnv().initBeforeClass();
  ... // data preparation
}
```

After the test, please clean up the environment by shut down the connections that have not been closed. This logic should be implemented in the ```tearDown()``` method. The ```tearDown()``` method follows the annotation ```@AfterClass``` or ```@After```. The former means that this method is the last method to execute for the IT class and is executed only once. The latter indicates that ```tearDown()``` will be executed after each test method in the IT class. 

- If the IoTDB connection is declared as an instance variable and is not closed after the test, please explicitly close it in the ```tearDown()``` method.
- The cleaning up should be implemented through the factory class, i.e., ```EnvFactory.getEnv().cleanAfterClass()```. 


```java
@AfterClass
public static void tearDown() throws Exception {
  ... // close the connection
  // clean up the environment
  EnvFactory.getEnv().cleanAfterClass();
}
```

#### 3. Implementing the logic of IT

IT of Apache IoTDB should be implemented as black-box testing. Please name the method as "functionality"+"Test", e.g., "<span style="color:green">selectWithAlias</span><span style="color:red">Test</span>". The interaction should be implemented through JDBC or Session API. 

1 With JDBC

When using the JDBC interface, it is recommended that the connection be established in a try statement. Connections established in this way do not need to be closed in the tearDown method explicitly. Connections need to be established through the factory class, i.e., ```EnvFactory.getEnv().getConnection()```. It is not necessary to specify the IP address or port number. The sample code is shown below.

```java
@Test
public void someFunctionTest(){
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      ... // execute the statements and test the correctness
    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail();
    }
}
```

Please note that,
- **It is required to use ```executeQuery()``` to query the data from the database and get the ResultSet.**
- **For updating the database without any return value, it is required to use ```execute()``` method to interact with the database.** 
The sample code is as follows.

```java
@Test
public void exampleTest() throws Exception {
  try (Connection connection = EnvFactory.getEnv().getConnection();
      Statement statement = connection.createStatement()) {
    // use execute() to set the databases
    statement.execute("CREATE DATABASE root.sg");
    // use executeQuery() query the databases
    try (ResultSet resultSet = statement.executeQuery("SHOW DATABASES")) {
      if (resultSet.next()) {
        String storageGroupPath = resultSet.getString("database");
        Assert.assertEquals("root.sg", storageGroupPath);
      } else {
        Assert.fail("This ResultSet is empty.");
      }
    }
  }
}
```

2 With Session API

Currently, it is not recommended to implement IT with Session API. 

3 Annotations of Environment for the Test Methods

For test methods, developers can also specify a test environment with the annotation before the method. It is important to note that a case with additional test environment annotations will be tested not only in the specified environment, but also in the environment of the IT class to which the use case belongs. The sample code is as follows.


```java
@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class})
public class IoTDBExampleIT {

 // This case will only be tested in a local stand-alone test environment
 @Test
 public void theStandaloneCaseTest() {
   ...
 }

 // The use case will be tested in the local standalone environment, the cluster environment, and the remote test environment.
 @Test
 @Category({ClusterIT.class, RemoteIT.class})
 public void theAllEnvCaseTest() {
   ...
 }
}
```

#### 4. Change the configurations of IoTDB when testing

Sometimes, the configurations of IoTDB need to be changed in order to test the functionalities under certain conditions. Because changing the configurations on a remote machine is troublesome, configuration modification is not allowed in the remote environment. However, it is allowed in the local standalone and  cluster environment. Changes of the configuration files should be implemented in the ```setUp()``` method, before ```EnvFactory.getEnv().initBeforeClass()```, and should be implemented through ConfigFactory. In  ```tearDown()``` , please undo all changes of the configurations and revert to its original default settings by ConfigFactory after the environment cleanup (```EnvFactory.getEnv().cleanAfterTest()```). The example code is as follows.


```java
@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class, ClusterIT.class})
public class IoTDBAlignedSeriesQueryIT {

  protected static boolean enableSeqSpaceCompaction;
  protected static boolean enableUnseqSpaceCompaction;
  protected static boolean enableCrossSpaceCompaction;

  @BeforeClass
  public static void setUp() throws Exception {
    // get the default configurations
    enableSeqSpaceCompaction = ConfigFactory.getConfig().isEnableSeqSpaceCompaction();
    enableUnseqSpaceCompaction = ConfigFactory.getConfig().isEnableUnseqSpaceCompaction();
    enableCrossSpaceCompaction = ConfigFactory.getConfig().isEnableCrossSpaceCompaction();
    // update configurations
    ConfigFactory.getConfig().setEnableSeqSpaceCompaction(false);
    ConfigFactory.getConfig().setEnableUnseqSpaceCompaction(false);
    ConfigFactory.getConfig().setEnableCrossSpaceCompaction(false);
    EnvFactory.getEnv().initBeforeClass();
    AlignedWriteUtil.insertData();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanAfterClass();
    // revert to the default configurations 
    ConfigFactory.getConfig().setEnableSeqSpaceCompaction(enableSeqSpaceCompaction);
    ConfigFactory.getConfig().setEnableUnseqSpaceCompaction(enableUnseqSpaceCompaction);
    ConfigFactory.getConfig().setEnableCrossSpaceCompaction(enableCrossSpaceCompaction);
  }
}
```


## Q&A
### Ways to check the log after the CI failure
1 click *Details* of the corresponding test

<img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://github.com/apache/iotdb-bin-resources/blob/main/integration-test/pic/details.png">

2 check and download the error log

<img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://github.com/apache/iotdb-bin-resources/blob/main/integration-test/pic/download1.png">

You can also click the *summary* at the upper left and then check and download the error log.

<img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://github.com/apache/iotdb-bin-resources/blob/main/integration-test/pic/download2.png">

### Commands for running IT

Please check [Integration Test For the MPP Architecture](https://github.com/apache/iotdb/blob/master/integration-test/README.md) for details.