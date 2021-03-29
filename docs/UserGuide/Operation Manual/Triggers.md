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



# Triggers



## Triggers Management

You can register, deregister, start or stop a trigger instance through SQL statements, and you can also query all registered triggers through SQL statements.

Triggers have two states: `STARTED` and `STOPPED`. You can start or stop a trigger by executing `START TRIGGER` or `STOP TRIGGER`. Note that the triggers registered by the `CREATE TRIGGER` statement are `STARTED` by default.



### Create Triggers

The following shows the SQL syntax of how to register a trigger.

```sql
CREATE TRIGGER <TRIGGER-NAME>
(BEFORE | AFTER) INSERT
ON <FULL-PATH>
AS <CLASSNAME>
```

You can also set any number of key-value pair attributes for the trigger through the `WITH` clause:

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

Note that `CLASSNAME`, `KEY` and `VALUE` in key-value pair attributes need to be quoted in single or double quotes.



### Drop Triggers

The following shows the SQL syntax of how to deregister a trigger.

```sql
DROP TRIGGER <TRIGGER-NAME>
```



### Start Triggers

The following shows the SQL syntax of how to start a trigger.

```sql
START TRIGGER <TRIGGER-NAME>
```



### Stop Triggers

The following shows the SQL syntax of how to stop a trigger.

```sql
STOP TRIGGER <TRIGGER-NAME>
```



### Show All Registered Triggers

``` sql
SHOW TRIGGERS
```



## Utilities

Utility classes provide programming paradigms and execution frameworks for the common requirements, which can simplify part of your work of implementing triggers.



### Windowing Utility

The windowing utility can help you define sliding windows and the data processing logic on them. It can construct two types of sliding windows: one has a fixed time interval (`SlidingTimeWindowEvaluationHandler`), and the other has fixed number of data points (`SlidingSizeWindowEvaluationHandler`).

The windowing utility allows you to define a hook (`Evaluator`) on the window (`Window`). Whenever a new window is formed, the hook you defined will be called once. You can define any data processing-related logic in the hook. The call of the hook is asynchronous. Therefore, the current thread will not be blocked when the window processing logic is executed.

For the definition of `Window` and `Evaluator`, please refer to the `org.apache.iotdb.db.utils.windowing.api` package.



#### SlidingSizeWindowEvaluationHandler

##### Window Construction

There are two construction methods.

The first method requires you to provide the type of data points that the window collects, the window size, the sliding step, and a hook (`Evaluator`).

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

The second method requires you to provide the type of data points that the window collects, the window size, and a hook (`Evaluator`). The sliding step is equal to the window size by default.

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

The window size and the sliding step must be positive.



#####  Datapoint Collection

``` java
final long timestamp = 0;
final int value = 0;
hander.collect(timestamp, value);
```

Note that the type of the second parameter accepted by the `collect` method needs to be consistent with the `dataType` parameter provided during construction.



#### SlidingTimeWindowEvaluationHandler

##### Window Construction

There are two construction methods.

The first method requires you to provide the type of data points that the window collects, the time interval, the sliding step, and a hook (`Evaluator`).

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

The second method requires you to provide the type of data points that the window collects, the time interval, and a hook (`Evaluator`). The sliding step is equal to the time interval by default.

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

The time interval and the sliding step must be positive.



#####  Datapoint Collection

``` java
final long timestamp = 0;
final int value = 0;
hander.collect(timestamp, value);
```

Note that the type of the second parameter accepted by the `collect` method needs to be consistent with the `dataType` parameter provided during construction.



#### Rejection Policy

The execution of window evaluation tasks is asynchronous.

When asynchronous tasks cannot be consumed by the execution thread pool in time, tasks will accumulate. In extreme cases, the accumulation of asynchronous tasks can cause the system OOM. Therefore, the number of tasks that the window evaluation thread pool allows to accumulate is set to a finite value.

When the number of accumulated tasks exceeds the limit, the newly submitted tasks will not be able to enter the thread pool for execution. At this time, the system will call the rejection policy hook `onRejection` that you have implemented in the listening hook (`Evaluator`) for processing.

The default behavior of `onRejection` is as follows.

````java
default void onRejection(Window window) {
  throw new RejectedExecutionException();
}
````

The way to implement a rejection strategy hook is as follows.

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



#### Configurable Properties

##### concurrent_window_evaluation_thread

The number of threads that can be used for evaluating sliding windows. The value is equals to CPU core number by default.



##### max_pending_window_evaluation_tasks

The maximum number of window evaluation tasks that can be pending for execution. The value is 64 by default.