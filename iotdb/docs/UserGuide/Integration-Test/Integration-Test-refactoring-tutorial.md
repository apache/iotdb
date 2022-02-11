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

# Integration Test refactoring tutorial

- Step 0. Prerequisites

  - **IT Location has been moved**;  all Integration Tests have been moved to the integration module.
  - **Test case MUST label**; the test classification label is the junit's `category`, which determines the test environment or process in which the test case is tested.
  - **Code related to the test environment MAY need to be refactored**; this determines whether the current test environment can correctly test the test case. The corresponding statement needs to be refactored.



- Step 1. Labeling test cases

  - Add the appropriate `Category` before the test case class or test case method, which can collect any desired test category labels.

  - The `Category` of the following three test categories are all real and effective,

    ```java
    @Category({LocalStandaloneTest.class, ClusterTest.class, RemoteTest.class})
    public class IoTDBAliasIT {
      ......
    }
    
    
    @Category({LocalStandaloneTest.class, ClusterTest.class})
    public class IoTDBAlignByDeviceIT {
      ......
    }
    
    
    @Category({LocalStandaloneTest.class})
    public class IoTDBArithmeticIT {
      ......
    }
    ```

  - You can also add `Category` at the test method level.

    ```java
    @Category({LocalStandaloneTest.class})
    public class IoTDBExampleIT {
    
     // This case can ONLY test in environment of local.
     @Test
     public void theStandaloneCase() {
       ......
     }
    
     // This case can test in environment of local, cluster and remote.
     @Test
     @Category({ClusterTest.class, RemoteTest.class})
     public void theAllEnvCase() {
       ......
     }
    }
    ```

  - At present, all test cases must at least add the `Category` of the stand-alone test, namely `LocalStandaloneTest.class`.



- Step 2. Environmental code refactoring

  - If the test case needs to be tested in the Cluster or Remote environment, the environment-related code MUST be refactored accordingly. If it is only tested in the LocalStandalone environment, modifications are only recommended. (Not all test cases can be tested in the Cluster or Remote environment because statements that are limited by some functions, such as local file operations, cannot be refactored.)

    |                            | LocalStandalone |   Cluster   |   Remote    |
    | :------------------------- | :-------------: | :---------: | :---------: |
    | setUp and tearDown         |    Recommend    |    Must     |    Must     |
    | getConnection              |    Recommend    |    Must     |    Must     |
    | change config              |    Recommend    |    Must     | Not support |
    | Local file operation       |  Won't change   | Not support | Not support |
    | Local descriptor operation |  Won't change   | Not support | Not support |
    | restart operation          |  Won't change   | Not support | Not support |

    

  - The `setUp` and `tearDown` methods must be refactored in the Cluster and Remote environment

    ```java
    @Category({LocalStandaloneTest.class, ClusterTest.class, RemoteTest.class})
    public class IoTDBAliasIT {
    
      @BeforeClass
      public static void setUp() throws Exception {
        // EnvironmentUtils.closeStatMonitor(); // orginal setup code
        // EnvironmentUtils.envSetUp(); // orginal setup code
        EnvFactory.getEnv().initBeforeClass(); // new initBeforeClass code
    
        insertData();
      }
    
      @AfterClass
      public static void tearDown() throws Exception {
        // EnvironmentUtils.cleanEnv(); // orginal code
        EnvFactory.getEnv().cleanAfterClass(); // new cleanAfterClass code
      }
    }
    ```

    

  - The `getConnection` must be refactored in Cluster and Remote environments

    ```java
    private static void insertData() throws ClassNotFoundException {
        // Class.forName(Config.JDBC_DRIVER_NAME); // orginal connection code
        // try (Connection connection =  
        //         DriverManager.getConnection( 
        //             Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        try (Connection connection = EnvFactory.getEnv().getConnection(); // new code
            Statement statement = connection.createStatement()) {
    
          for (String sql : sqls) {
            statement.execute(sql);
          }
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
    ```
  
  - The method of changing the IoTDB configuration must be refactored in the Cluster environment. (As the remote environment cannot change the configuration remotely at present, the test cases that change the configuration will not support testing in the remote environment)
    - In the Cluster environment, as the configuration cannot be changed dynamically, only the configuration changes before the environment init are effective.
    - The refactoring has included most of the configuration changes, which can be changed through the method of `ConfigFactory.getConfig()`.

    ```java
      @Category({LocalStandaloneTest.class, ClusterTest.class})
      public class IoTDBCompleteIT {
        private int prevVirtualStorageGroupNum;
    
        @Before
        public void setUp() {
          prevVirtualStorageGroupNum =
              IoTDBDescriptor.getInstance().getConfig().getVirtualStorageGroupNum();
          // IoTDBDescriptor.getInstance().getConfig().setVirtualStorageGroupNum(16); // orginal code
          ConfigFactory.getConfig().setVirtualStorageGroupNum(16); // new code
          EnvFactory.getEnv().initBeforeClass();
        }
    ```
  
    - If the configuration item has not been included in the method of `ConfigFactory.getConfig()`, it needs to be defined in the BaseConfig.java interface file and implemented in StandaloneEnvConfig.java and ClusterEnvConfig.java, respectively. This part is not very common. For specific, please refer to the realized part.
