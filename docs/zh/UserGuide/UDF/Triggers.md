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



# 触发器

触发器提供了一种侦听序列数据变动的机制。配合用户自定义逻辑，可完成告警、数据清洗、数据转发等功能。

触发器基于Java反射机制实现。用户通过简单实现Java接口，即可实现数据侦听。IoTDB允许用户动态装载、卸载触发器，在装载、卸载期间，无需启停服务器。

根据此文档，您将会很快学会触发器的编写与管理。



## 编写触发器

### 触发器依赖

触发器的逻辑需要您编写Java类进行实现。

在编写触发器逻辑时，需要使用到下面展示的依赖。如果您使用[Maven](http://search.maven.org/)，则可以直接从[Maven库](http://search.maven.org/)中搜索到它们。

``` xml
<dependency>
  <groupId>org.apache.iotdb</groupId>
  <artifactId>iotdb-server</artifactId>
  <version>0.12.0-SNAPSHOT</version>
  <scope>provided</scope>
</dependency>
```

请注意选择和目标服务器版本相同的依赖版本。



### 用户编程接口

编写一个触发器需要实现`org.apache.iotdb.db.engine.trigger.api.Trigger`类。

该类提供了两类编程接口：**生命周期钩子**和**数据变动侦听钩子**。该类中所有的接口都不是必须实现的，当您不实现它们时，它们不会对流经的数据操作产生任何响应。您可以根据实际需要，只实现其中若干接口。

下面是所有可供用户进行实现的接口的说明。



#### 生命周期钩子

| 接口定义                                                     | 描述                                                         |
| :----------------------------------------------------------- | ------------------------------------------------------------ |
| `void onCreate(TriggerAttributes attributes) throws Exception` | 当您使用`CREATE TRIGGER`语句注册触发器后，该钩子会被调用一次。在每一个实例的生命周期内，该钩子会且仅仅会被调用一次。该钩子主要有如下作用：1. 帮助用户解析SQL语句中的自定义属性（使用`TriggerAttributes`）。 2. 创建或申请资源，如建立外部链接、打开文件等。 |
| `void onDrop() throws Exception`                             | 当您使用`DROP TRIGGER`语句删除触发器后，该钩子会被调用。在每一个实例的生命周期内，该钩子会且仅仅会被调用一次。该钩子的主要作用是进行一些资源释放等的操作。 |
| `void onStart() throws Exception`                            | 当您使用`START TRIGGER`语句手动启动（被`STOP TRIGGER`语句停止的）触发器后，该钩子会被调用。 |
| `void onStop() throws Exception`                             | 当您使用`STOP TRIGGER`语句手动停止触发器后，该钩子会被调用。 |



#### 数据变动侦听钩子

目前触发器仅能侦听数据插入的操作。

数据变动侦听钩子的调用时机由`CREATE TRIGGER`语句显式指定，在编程接口层面不作区分。



##### 单点数据插入侦听钩子

``` java
Integer fire(long timestamp, Integer value) throws Exception;
Long fire(long timestamp, Long value) throws Exception;
Float fire(long timestamp, Float value) throws Exception;
Double fire(long timestamp, Double value) throws Exception;
Boolean fire(long timestamp, Boolean value) throws Exception;
Binary fire(long timestamp, Binary value) throws Exception;
```

对于注册序列上的每一点数据插入，触发器都会调用`fire`作为响应，钩子的入参`timestamp`和`value`即是这一次插入数据点的时间和数据值。您可以在`fire`钩子中编写处理数据的任意逻辑。

注意，目前钩子的返回值是没有任何意义的。



##### 批量数据插入侦听钩子

```java
int[] fire(long[] timestamps, int[] values) throws Exception;
long[] fire(long[] timestamps, long[] values) throws Exception;
float[] fire(long[] timestamps, float[] values) throws Exception;
double[] fire(long[] timestamps, double[] values) throws Exception;
boolean[] fire(long[] timestamps, boolean[] values) throws Exception;
Binary[] fire(long[] timestamps, Binary[] values) throws Exception;
```

如果您需要在业务场景中使用到Session API的`insertTablet`接口或`insertTablets`接口，那么您可以通过实现上述数据插入的侦听钩子来降低触发器的调用开销。

推荐您在实现上述批量数据插入的侦听钩子时， 保证批量数据插入侦听钩子与单点数据插入侦听钩子的行为具有一致性。当您不实现批量数据插入的侦听钩子时，它将遵循下面的默认逻辑。

```java
default int[] fire(long[] timestamps, int[] values) throws Exception {
  int size = timestamps.length;
  for (int i = 0; i < size; ++i) {
    fire(timestamps[i], values[i]);
  }
  return values;
}
```

注意，目前钩子的返回值是没有任何意义的。



#### 重要注意事项

* 每条序列上注册的触发器都是一个完整的触发器类的实例，因此您可以在触发器中维护一些状态数据。
* 触发器维护的状态会在系统停止后被清空（除非您在钩子中主动将状态持久化）。换言之，系统启动后触发器的状态将会默认为初始值。
* 一个触发器所有钩子的调用都是串行化的。



## 管理触发器

您可以通过SQL语句注册、卸载、启动或停止一个触发器实例，您也可以通过SQL语句查询到所有已经注册的触发器。



### 触发器的状态

触发器有两种运行状态：`STARTED`和`STOPPED`，您需要执行`START TRIGGER`或者`STOP TRIGGER`来启动或者停止一个触发器。

当一个触发器的状态为`STOPPED`时，它将不会响应被注册序列上的操作（如插入数据点的操作），对外表现就会像是这个序列没有被注册过触发器一样，但是它会保存所有的状态（触发器类变量）信息，同时也会保存所有的注册信息。

注意，通过`CREATE TRIGGER`语句注册的触发器默认是`STARTED`的。



### 注册触发器

触发器只能注册在一个已经存在的时间序列上。任何时间序列只允许注册一个触发器。

被注册有触发器的序列将会被触发器侦听，当序列上有数据变动时，触发器中对应的钩子将会被调用。



注册一个触发器可以按如下流程进行：

1. 实现一个完整的Trigger类，假定这个类的全类名为`org.apache.iotdb.db.engine.trigger.example.AlertListener`

2. 将项目打成JAR包，如果您使用Maven管理项目，可以参考上述Maven项目示例的写法

3. 将JAR包放置到目录 `iotdb-server-0.12.0-SNAPSHOT/ext/trigger` （也可以是`iotdb-server-0.12.0-SNAPSHOT/ext/trigger`的子目录）下。

   > 您可以通过修改配置文件中的`trigger_root_dir`来指定加载触发器JAR包的根路径。

4. 使用SQL语句注册该触发器，假定赋予该触发器的名字为`alert-listener-sg1d1s1`

5. 使用`CREATE TRIGGER`语句注册该触发器

   ```sql
   CREATE TRIGGER alert-listener-sg1d1s1
   AFTER INSERT
   ON root.sg1.d1.s1
   AS "org.apache.iotdb.db.engine.trigger.example.AlertListener"
   WITH (
     "lo" = "0", 
     "hi" = "100.0"
   )
   ```

   

注册触发器的详细SQL语法如下：

```sql
CREATE TRIGGER <TRIGGER-NAME>
(BEFORE | AFTER) INSERT
ON <FULL-PATH>
AS <CLASSNAME>
```

同时，您还可以通过`WITH`子句传入任意数量的自定义属性值：

```sql
CREATE TRIGGER <TRIGGER-NAME>
(BEFORE | AFTER) INSERT
ON <FULL-PATH>
AS <CLASSNAME>
WITH (
  <KEY-1>=<VALUE-1>, 
  <KEY-2>=<VALUE-2>, 
  ...
)
```

`TRIGGER-NAME`是用于标定触发器的全局唯一ID，它是大小写敏感的。

目前触发器可以侦听序列上的所有的数据插入操作，触发器可以选择在数据插入前（`BEFORE INSERT`）或者数据插入后（`AFTER INSERT`）触发钩子调用。

`FULL-PATH`是触发器侦听的目标序列名称，这个序列必须是一个测点。

`CLASSNAME`是触发器类的全类名。

请注意，`CLASSNAME`以及属性值中的`KEY`和`VALUE`都需要被单引号或者双引号引用起来。



### 卸载触发器

触发器会在下面几种情景下被卸载：

1. 用户执行`DELETE TIMESERIES`时，序列上注册的触发器会被卸载
2. 用户执行`DELETE STORAGE GROUP`时，对应存储组下注册的触发器会全部被卸载
3. 用户使用`DROP TRIGGER`语句主动卸载



卸载触发器的SQL语法如下：

```sql
DROP TRIGGER <TRIGGER-NAME>
```

`TRIGGER-NAME`是用于标定触发器的全局唯一ID。



下面是一个`DROP TRIGGER`语句的例子：

```sql
DROP TRIGGER alert-listener-sg1d1s1
```



### 启动触发器

该操作是“停止触发器”的逆操作。它将运行状态为`STOPPED`的触发器的运行状态变更为`STARTED`，这会使得触发器重新侦听被注册序列上的操作，并对数据变动产生响应。



启动触发器的SQL语法如下：

```sql
START TRIGGER <TRIGGER-NAME>
```

`TRIGGER-NAME`是用于标定触发器的全局唯一ID。



下面是一个`START TRIGGER`语句的例子：

```sql
START TRIGGER alert-listener-sg1d1s1
```



注意，通过`CREATE TRIGGER`语句注册的触发器默认是`STARTED`的。



### 停止触发器

该操作将触发器的状态由`STARTED`变为`STOPPED`。当一个触发器的状态为`STOPPED`时，它将不会响应被注册序列上的操作（如插入数据点的操作），对外表现就会像是这个序列没有被注册过触发器一样。您可以使用`START TRIGGER`语句重新启动一个触发器。



停止触发器的SQL语法如下：

```sql
STOP TRIGGER <TRIGGER-NAME>
```

`TRIGGER-NAME`是用于标定触发器的全局唯一ID。



下面是一个`STOP TRIGGER`语句的例子：

```sql
STOP TRIGGER alert-listener-sg1d1s1
```



### 查询所有注册的触发器

查询触发器的SQL语句如下：

``` sql
SHOW TRIGGERS
```

该语句展示已注册触发器的ID、运行状态、触发时机、被注册的序列、触发器实例的全类名和注册触发器时用到的自定义属性。



### 用户权限管理

用户在使用触发器时会涉及到4种权限：

* `CREATE_TRIGGER`：具备该权限的用户才被允许注册触发器操作。
* `DROP_TRIGGER`：具备该权限的用户才被允许卸载触发器操作。
* `START_TRIGGER`：具备该权限的用户才被允许启动已被停止的触发器。
* `STOP_TRIGGER`：具备该权限的用户才被允许停止正在运行的触发器。

更多用户权限相关的内容，请参考[权限管理语句](../Operation%20Manual/Administration.md)。



### 重要注意事项

* 触发器是通过反射技术动态装载的，因此您在装载过程中无需启停服务器。

* 不同的JAR包中最好不要有全类名相同但功能实现不一样的类。例如：触发器`trigger1`、`trigger2`分别对应资源`trigger1.jar`、`trigger2.jar`。如果两个JAR包里都包含一个`org.apache.iotdb.db.engine.trigger.example.AlertListener`类，当`CREATE TRIGGER`使用到这个类时，系统会随机加载其中一个JAR包中的类，最终导致触发器执行行为不一致以及其他的问题。

* 拥有同一个全类名的触发器类的版本管理问题。IoTDB不允许系统中存在拥有同一全类名但是版本（逻辑）不一样的触发器。

  相关问题：IoTDB预先注册了10个`org.apache.iotdb.db.engine.trigger.example.AlertListener`触发器实例，DBA更新了`org.apache.iotdb.db.engine.trigger.example.AlertListener`的实现和对应的JAR包，是否可以只卸载其中5个，将这5个替换为新的实现？

  回答：无法做到。只有将预先注册的10个触发器全部卸载，才能装载到新的触发器实例。在原有触发器没有全部被卸载的情况下，新注册的拥有相同全类名的触发器行为只会与现有触发器的行为一致。


