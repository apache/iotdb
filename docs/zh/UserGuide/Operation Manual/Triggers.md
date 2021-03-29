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



## SQL管理语句

您可以通过SQL语句注册、卸载、启动或停止一个触发器实例，您也可以通过SQL语句查询到所有已经注册的触发器。

触发器有两种运行状态：`STARTED`和`STOPPED`，您可以通过执行`START TRIGGER`或者`STOP TRIGGER`启动或者停止一个触发器。注意，通过`CREATE TRIGGER`语句注册的触发器默认是`STARTED`的。



### 注册触发器

注册触发器的SQL语法如下：

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

注意，`CLASSNAME`以及属性值中的`KEY`和`VALUE`都需要被单引号或者双引号引用起来。



### 卸载触发器

卸载触发器的SQL语法如下：

```sql
DROP TRIGGER <TRIGGER-NAME>
```



### 启动触发器

启动触发器的SQL语法如下：

```sql
START TRIGGER <TRIGGER-NAME>
```



### 停止触发器

停止触发器的SQL语法如下：

```sql
STOP TRIGGER <TRIGGER-NAME>
```



### 查询所有注册的触发器

``` sql
SHOW TRIGGERS
```



## 实用工具类

实用工具类为常见的需求提供了编程范式和执行框架，它能够简化您编写触发器的一部分工作。



### 窗口工具类

窗口工具类能够辅助您定义滑动窗口以及窗口上的数据处理逻辑。它能够构造两类滑动窗口：一种滑动窗口是固定窗口内时间长度的（`SlidingTimeWindowEvaluationHandler`），另一种滑动窗口是固定窗口内数据点数的（`SlidingSizeWindowEvaluationHandler`）。

窗口工具类允许您在窗口（`Window`）上定义侦听钩子（`Evaluator`）。每当一个新的窗口形成，您定义的侦听钩子就会被调用一次。您可以在这个侦听钩子内定义任何数据处理相关的逻辑。侦听钩子的调用是异步的，因此，在执行钩子内窗口处理逻辑的时候，是不会阻塞当前线程的。

`Window`与`Evaluator`接口的定义见`org.apache.iotdb.db.utils.windowing.api`包。



#### 固定窗口内数据点数的滑动窗口

##### 窗口构造

共两种构造方法。

第一种方法需要您提供窗口接受数据点的类型、窗口大小、滑动步长和一个侦听钩子（`Evaluator`）。

``` java
final TSDataType dataType = TSDataType.INT32;
final int windowSize = 10;
final int slidingStep = 5;

SlidingSizeWindowEvaluationHandler handler =
	  new SlidingSizeWindowEvaluationHandler(
        new SlidingSizeWindowConfiguration(dataType, windowSize, slidingStep),
        window -> {
          // do something
        });
```

第二种方法需要您提供窗口接受数据点的类型、窗口大小和一个侦听钩子（`Evaluator`）。这种构造方法下的窗口滑动步长等于窗口大小。

``` java
final TSDataType dataType = TSDataType.INT32;
final int windowSize = 10;

SlidingSizeWindowEvaluationHandler handler =
	  new SlidingSizeWindowEvaluationHandler(
        new SlidingSizeWindowConfiguration(dataType, windowSize),
        window -> {
          // do something
        });
```

窗口大小、滑动步长必须为正数。



#####  数据接收

``` java
final long timestamp = 0;
final int value = 0;
hander.collect(timestamp, value);
```

注意，`collect`方法接受的第二个参数类型需要与构造时传入的`dataType`声明一致。



#### 固定窗口内时间长度的滑动窗口

##### 窗口构造

共两种构造方法。

第一种方法需要您提供窗口接受数据点的类型、窗口内时间长度、滑动步长和一个侦听钩子（`Evaluator`）。

``` java
final TSDataType dataType = TSDataType.INT32;
final long timeInterval = 1000;
final long slidingStep = 500;

SlidingTimeWindowEvaluationHandler handler =
    new SlidingTimeWindowEvaluationHandler(
        new SlidingTimeWindowConfiguration(dataType, timeInterval, slidingStep),
        window -> {
          // do something
        });
```

第二种方法需要您提供窗口接受数据点的类型、窗口内时间长度和一个侦听钩子（`Evaluator`）。这种构造方法下的窗口滑动步长等于窗口内时间长度。

``` java
final TSDataType dataType = TSDataType.INT32;
final long timeInterval = 1000;

SlidingTimeWindowEvaluationHandler handler =
    new SlidingTimeWindowEvaluationHandler(
        new SlidingTimeWindowConfiguration(dataType, timeInterval),
        window -> {
          // do something
        });
```

窗口内时间长度、滑动步长必须为正数。



#####  数据接收

``` java
final long timestamp = 0;
final int value = 0;
hander.collect(timestamp, value);
```

注意，`collect`方法接受的第二个参数类型需要与构造时传入的`dataType`声明一致。



#### 拒绝策略

窗口计算的任务执行是异步的。

当异步任务无法被执行线程池及时消费时，会产生任务堆积。在极端情况下，异步任务的堆积会导致系统OOM。因此，窗口计算线程池允许堆积的任务数量被设定为有限值。

当堆积的任务数量超出限值时，新提交的任务将无法进入线程池执行，此时，系统会调用您在侦听钩子（`Evaluator`）中制定的拒绝策略钩子`onRejection`进行处理。

`onRejection`的默认行为如下。

````java
default void onRejection(Window window) {
  throw new RejectedExecutionException();
}
````

制定拒绝策略钩子的方式如下。

```java
SlidingTimeWindowEvaluationHandler handler =
    new SlidingTimeWindowEvaluationHandler(
        new SlidingTimeWindowConfiguration(TSDataType.INT32, 1, 1),
        new Evaluator() {
          @Override
          public void evaluate(Window window) {
            // do something
	        }

  	      @Override
    	    public void onRejection(Window window) {
      	  	// do something
        	}
    });
```



#### 配置参数

##### concurrent_window_evaluation_thread

窗口计算线程池的默认线程数。默认为CPU核数。



##### max_pending_window_evaluation_tasks

最多允许堆积的窗口计算任务。默认为64个。

