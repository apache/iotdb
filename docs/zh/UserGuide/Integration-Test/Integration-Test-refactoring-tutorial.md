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

# 集成测试开发者文档

**集成测试**是软件测试中的一个阶段。在该阶段中，各个软件模块被组合起来作为一个整体进行测试。进行集成测试是为了评估某系统或某组件是否符合指定的功能需求。


## Apache IoTDB 集成测试规范

### Apache IoTDB 集成测试的环境

Apache IoTDB 集成测试的环境一共有3种，分别为**本地单机测试环境、本地集群测试环境和远程测试环境。** Apache IOTDB 的集群测试需要在其中的1种或多种环境下完成。对于这三类环境的说明如下：
1. 本地单机测试环境：该环境用于完成本地的 Apache IoTDB 单机版的集成测试。若需要变更该环境的具体配置，需要在 IoTDB 实例启动前替换相应的配置文件，再启动 IoTDB 并进行测试。
2. 本地集群测试环境：该环境用于完成本地的 Apache IoTDB 分布式版（伪分布式）的集成测试。若需要变更该环境的具体配置，需要在 IoTDB 集群启动前替换相应的配置文件，再启动 IoTDB 集群并进行测试。
3. 远程测试环境：该环境用于测试远程 Apache IoTDB 的功能，连接的 IoTDB 实例可能是一个单机版的实例，也可以是远程集群的某一个节点。远程测试环境的具体配置的修改受到限制，暂不支持在测试时修改。
集成测试开发者在编写测试程序时需要指定这三种环境的1种或多种。具体指定方法见后文。

### 黑盒测试

**黑盒测试** 是一种软件测试方法，它检验程序的功能，而不考虑其内部结构或工作方式。开发者不需要了解待测程序的内部逻辑即可完成测试。**Apache IoTDB 的集成测试以黑盒测试的方式进行。通过 JDBC 或 Session API 的接口实现测试输入的用例即为黑盒测试用例。** 因此，测试用例的输出验证也应该通过 JDBC 或 Session API 的返回结果实现。

### 集成测试的步骤

集成测试的步骤主要分为三步，即 (1) 构建测试类和标注测试环境、(2) 设置测试前的准备工作以及测试后的清理工作以及 (3) 实现集成测试逻辑。如果需要测试非默认环境下的 IoTDB，还需要修改 IoTDB 的配置，修改方法对应小结的第4部分。

<img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://github.com/apache/iotdb-bin-resources/blob/main/integration-test/pic/step.svg">

#### 1. 集成测试类和注解

构建的集成测试类时，开发者需要在 Apache IoTDB 的 [integration-test](https://github.com/apache/iotdb/tree/master/integration-test) 模块中创建测试类。类名应当能够精简准确地表述该集成测试的目的。除用于服务其他测试用例的类外，含集成测试用例用于测试 Apache IoTDB 功能的类，应当命名为“功能+IT”。例如，用于测试IoTDB自动注册元数据功能的集成测试命名为“<span style="color:green">IoTDBAutoCreateSchema</span><span style="color:red">IT</span>”。

- Category 注解：**在构建集成测试类时，需要显式地通过引入```@Category```注明测试环境** ，测试环境用```LocalStandaloneIT.class```、```ClusterIT.class``` 和 ```RemoteIT.class```来表示，分别与“Apache IoTDB 集成测试的环境”中的本地单机测试环境、本地集群测试环境和远程测试环境对应。标签内是测试环境的集合，可以包含多个元素，表示在多种环境下分别测试。**一般情况下，标签```LocalStandaloneIT.class``` 和 ```ClusterIT.class``` 是必须添加的。** 当某些功能仅支持单机版 IoTDB 时可以只保留```LocalStandaloneIT.class```。
- RunWith 注解： 每一个集成测试类上都需要添加 ```@RunWith(IoTDBTestRunner.class)``` 标签。

```java
// 给 IoTDBAliasIT 测试类加标签，分别在本地单机测试环境、
// 本地集群测试环境和远程测试环境完成测试。
@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class, ClusterIT.class, RemoteIT.class})
public class IoTDBAliasIT {
  ...
}

// 给 IoTDBAlignByDeviceIT 测试类加标签，分别在本地单机
// 测试环境和本地集群测试环境完成测试。
@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class, ClusterIT.class})
public class IoTDBAlignByDeviceIT {
  ...
}
```

#### 2. 设置测试前的准备工作以及测试后的清理工作

测试前的准备工作包括启动 IoTDB（单机或集群）实例和测试用的数据准备。这些逻辑在setUp方法内实现。其中setUp方法前需要添加```@BeforeClass``` 或 ```@Before``` 标签，前者表示该方法为当前集成测试执行的第 1 个方法，并且在集成测试运行时只执行 1 次，后者表示在运行当前集成测试的每 1 个测试方法前，该方法都会被执行 1 次。
- IoTDB 实例启动通过调用工厂类来实现，即```EnvFactory.getEnv().initBeforeClass()```。
- 测试用的数据准备包括按测试需要提前注册 database 、注册时间序列、写入时间序列数据等。建议在测试类内实现单独的方法来准备数据，如insertData()。若需要写入多条数据，请使用批量写入的接口（JDBC中的executeBatch接口，或Session API 中的 insertRecords、insertTablets 等接口）。

```java
@BeforeClass
public static void setUp() throws Exception {
  // 启动 IoTDB 实例
  EnvFactory.getEnv().initBeforeClass();
  ... // 准备数据
}
```

测试后需要清理相关的环境，其中需要断开还没有关闭的连接。这些逻辑在 tearDown 方法内实现。其中 tearDown 方法前需要添加```@AfterClass``` 或 ```@After``` 标签，前者表示该方法为当前集成测试执行的最后一个方法，并且在集成测试运行时只执行 1 次，后者表示在运行当前集成测试的每一个测试方法后，该方法都会被执行 1 次。
- 如果 IoTDB 连接以测试类成员变量的形式声明，并且在测试后没有断开连接，则需要在 tearDown 方法内显式断开。
- IoTDB 环境的清理通过调用工厂类来实现，即```EnvFactory.getEnv().cleanAfterClass()```。

```java
@AfterClass
public static void tearDown() throws Exception {
  ... // 断开连接等
  // 清理 IoTDB 实例的环境
  EnvFactory.getEnv().cleanAfterClass();
}
```

#### 3. 实现集成测试逻辑

Apache IoTDB 的集成测试以黑盒测试的方式进行，测试方法的名称为“测试的功能点+Test”，例如“<span style="color:green">selectWithAlias</span><span style="color:red">Test</span>”。测试通过 JDBC 或 Session API 的接口来完成。

1、使用JDBC接口

使用JDBC接口时，建议将连接建立在 try 语句内，以这种方式建立的连接无需在 tearDown 方法内关闭。连接需要通过工厂类来建立，即```EnvFactory.getEnv().getConnection()```，不要指定具体的 ip 地址或端口号。示例代码如下所示。

```java
@Test
public void someFunctionTest(){
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      ... // 执行相应语句并做测试
    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail();
    }
}
```
注意：
- **查询操作必须使用```executeQuery()```方法，返回ResultSet；** 对于**更新数据库等无返回值的操作，必须使用```execute()```方法。** 示例代码如下。

```java
@Test
public void exampleTest() throws Exception {
  try (Connection connection = EnvFactory.getEnv().getConnection();
      Statement statement = connection.createStatement()) {
    // 使用 execute() 方法设置存储组
    statement.execute("CREATE DATABASE root.sg");
    // 使用 executeQuery() 方法查询存储组
    try (ResultSet resultSet = statement.executeQuery("show databases")) {
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

2、使用 Session API

目前暂不支持使用 Session API 来做集成测试。

3、测试方法的环境标签
对于测试方法，开发者也可以指定特定的测试环境，只需要在对应的测试方法前注明环境即可。值得注意的是，有额外测试环境标注的用例，不但会在所指定的环境中进行测试，还会在该用例隶属的测试类所对应的环境中进行测试。示例代码如下。


```java
@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class})
public class IoTDBExampleIT {

 // 该用例只会在本地单机测试环境中进行测试
 @Test
 public void theStandaloneCaseTest() {
   ...
 }

 // 该用例会在本地单机测试环境、本地集群测试环境和远程测试环境中进行测试
 @Test
 @Category({ClusterIT.class, RemoteIT.class})
 public void theAllEnvCaseTest() {
   ...
 }
}
```

#### 4. 测试中 IoTDB 配置参数的修改

有时，为了测试 IoTDB 在特定配置条件下的功能需要更改其配置。由于远程的机器配置无法修改，因此，需要更改配置的测试不支持远程测试环境，只支持本地单机测试环境和本地集群测试环境。配置文件的修改需要在setUp方法中实现，在```EnvFactory.getEnv().initBeforeClass()```之前执行，应当使用 ConfigFactory 提供的方法来实现。在 tearDown 方法内，需要将 IoTDB 的配置恢复到原默认设置，这一步在环境清理（```EnvFactory.getEnv().cleanAfterTest()```）后通过调用ConfigFactory提供的方法来执行。实例代码如下。

```java
@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class, ClusterIT.class})
public class IoTDBAlignedSeriesQueryIT {

  protected static boolean enableSeqSpaceCompaction;
  protected static boolean enableUnseqSpaceCompaction;
  protected static boolean enableCrossSpaceCompaction;

  @BeforeClass
  public static void setUp() throws Exception {
    // 获取默认配置
    enableSeqSpaceCompaction = ConfigFactory.getConfig().isEnableSeqSpaceCompaction();
    enableUnseqSpaceCompaction = ConfigFactory.getConfig().isEnableUnseqSpaceCompaction();
    enableCrossSpaceCompaction = ConfigFactory.getConfig().isEnableCrossSpaceCompaction();
    // 更新配置
    ConfigFactory.getConfig().setEnableSeqSpaceCompaction(false);
    ConfigFactory.getConfig().setEnableUnseqSpaceCompaction(false);
    ConfigFactory.getConfig().setEnableCrossSpaceCompaction(false);
    EnvFactory.getEnv().initBeforeClass();
    AlignedWriteUtil.insertData();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanAfterClass();
    // 恢复为默认配置
    ConfigFactory.getConfig().setEnableSeqSpaceCompaction(enableSeqSpaceCompaction);
    ConfigFactory.getConfig().setEnableUnseqSpaceCompaction(enableUnseqSpaceCompaction);
    ConfigFactory.getConfig().setEnableCrossSpaceCompaction(enableCrossSpaceCompaction);
  }
}
```

## Q&A
### CI 出错后查看日志的方法
1、点击出错的测试对应的 Details

<img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://github.com/apache/iotdb-bin-resources/blob/main/integration-test/pic/details.png">

2、查看和下载日志

<img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://github.com/apache/iotdb-bin-resources/blob/main/integration-test/pic/download1.png">

也可以点击左上角的 summary 然后查看和下载其他错误日志。

<img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://github.com/apache/iotdb-bin-resources/blob/main/integration-test/pic/download2.png">

### 运行集成测试的命令

请参考 [《Integration Test For the MPP Architecture》](https://github.com/apache/iotdb/blob/master/integration-test/README.md) 文档。