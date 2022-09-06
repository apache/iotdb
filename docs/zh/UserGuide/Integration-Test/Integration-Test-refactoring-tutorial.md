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
# 集成测试
## IoTDB社区Integration Test改造说明

- 步骤0. 前提须知
  - **位置已移动**；所有的Integration Test已被移动至单独的integration模块。
  - **测试用例必须打分类标签**； `Category` 即测试分类标签，决定了该测试用例在哪套测试环境或流程中被测试。
  - **涉及测试环境的代码可能要重构**；决定了该测试用例是否能被当前测试环境正确测试，需要根据相应的环境重构相应的代码。



- 步骤1. 测试用例打标签

  - 在测试用例类或者测试用例方法前加上合适的`Category`，可以是任意期望的测试分类标签的集合。

  - 真实样例，下面三个测试类的`Category`都是真实有效的，

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
  
  - 甚至，你还可以在测试方法级别加`Category`。
  
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
  
  - 目前，所有测试用例至少要加上单机测试的测试分类，即`LocalStandaloneTest.class`。



- 步骤2. 环境代码重构

  - 如果测试用例需要在Cluster或者Remote环境下被测试，那么必须对环境相关的代码作相应重构，如果是仅在LocalStandalone环境下测试，则只推荐修改。（不是所有的测试用例可以在Cluster或者Remote环境下被测试，因为受限于部分功能的语句比如本地文件操作，这些代码不能被重构。）

    |                            | LocalStandalone |   Cluster   |   Remote    |
    | :------------------------- | :-------------: | :---------: | :---------: |
    | setUp and tearDown         |    Recommend    |    Must     |    Must     |
    | getConnection              |    Recommend    |    Must     |    Must     |
    | change config              |    Recommend    |    Must     | Not support |
    | Local file operation       |  Won't change   | Not support | Not support |
    | Local descriptor operation |  Won't change   | Not support | Not support |
    | restart operation          |  Won't change   | Not support | Not support |
  
    
  
  - `setUp` 和`tearDown` 方法内的重构，在Cluster和Remote环境下是必须更改的
  
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
  
    

  - `getConnection` 的重构，在Cluster和Remote环境下是必须更改
  
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
  
  
  
  - 更改配置的方法，在Cluster环境下是必须重构的。（由于目前Remote环境无法远程更改配置，更改配置的测试用例将不支持Remote环境下测试）
  
    - 在Cluster环境下，由于无法动态更改配置，只有环境init前的配置更改才有效。
    - 重构已包含了大部分的配置更改，通过`ConfigFactory.getConfig()` 的方法可以进行链式更改。
  
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
    
    - 若配置项尚未在`ConfigFactory.getConfig()` 的方法中包含，需要在BaseConfig.java接口文件中定义，在StandaloneEnvConfig.java和ClusterEnvConfig.java中分别实现，这部分不是很常用，具体方法可以参考已实现的部分，目前暂不列出。
