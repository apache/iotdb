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

# 原始数据查询

## 设计原理

原始数据查询根据是否包含值过滤条件，可以分为两类。不包含值过滤条件时，根据结果集结构又可分为两类。

* 不包含值过滤条件（无过滤条件 or 仅包含时间过滤条件）
	* 结果集按时间戳对齐（默认原始数据查询）
	* 结果集不按时间戳对齐（disable align）
* 包含值过滤条件
	* 结果集按时间戳对齐

以上三种查询在代码中对应三种不同的 DataSet，封装了这三种查询的执行逻辑。

## 不包含值过滤条件 + 结果集按时间戳对齐

### org.apache.iotdb.db.query.dataset.RawQueryDataSetWithoutValueFilter

`RawQueryDataSetWithoutValueFilter`实现了没有值过滤条件，且需要按照时间戳对齐的查询逻辑。虽然最后的查询结果需要每个时间序列按照时间戳对齐，但是每个时间序列的查询是可以做并行化的。这里借助消费者-生产者队列的思想，将每个时间序列获取数据的操作与最后对所有时间序列进行对齐的操作解耦。每个时间序列对应一个生产者线程，且有其对应的`BlockingQueue`，生产者任务负责读取相应的时间序列的数据放进`BlockingQueue`中；消费者线程只有一个，负责从每个时间序列的`BlockingQueue`中取出数据，进行时间戳对齐之后，将结果组装成`TSQueryDataSet`形式返回。

在具体实现的时候，考虑到机器的资源限制，并非为每个查询的每一个时间序列创建一个线程，而是采用线程池技术，将每一个时间序列的生产者任务当作一个`Runnable`的 task 提交到线程池中执行。

下面就先介绍生产者的代码，它被封装在是`RawQueryDataSetWithoutValueFilter`的一个内部类`ReadTask`中，实现了`Runnable`接口。

### org.apache.iotdb.db.query.dataset.RawQueryDataSetWithoutValueFilter.ReadTask

`ReadTask`中有两个字段

```
 private final ManagedSeriesReader reader;
 private BlockingQueue<BatchData> blockingQueue;
```

`ManagedSeriesReader`接口继承了`IBatchReader`接口，主要用来读取单个时间序列的数据，并且新增了以下四个方法

```
boolean isManagedByQueryManager();

void setManagedByQueryManager(boolean managedByQueryManager);

boolean hasRemaining();

void setHasRemaining(boolean hasRemaining);
```

前两个方法用以表征该时间序列对应的生产者任务有没有被查询管理器所管理，即生产者任务有没有因为阻塞队列满了，而自行退出（后文会解释为什么不阻塞等待，而是直接退出）；后两个方法用以表征该时间序列对应的 reader 里还有没有数据。

`blockingQueue`就是该生产者任务的阻塞队列，实际上该阻塞队列只会在消费者取数据时单边阻塞，生产者放数据时，如果发现队列满了，便会直接退出，不会阻塞。

下面看一下`ReadTask`的`run()`方法，执行流程的解释以注释的形式展现在代码中

#### run()

```
public void run() {
  try {
    // 这里加锁的原因是为了保证对于 blockingQueue 满不满的判断是正确同步的
    synchronized (reader) {
      // 由于每次提交生产者任务时（不论是生产者自己递归的提交自己，还是消费者发现生产者任务自行退出而提交的），都会检查队列是不是满的，如果队列满了，是不会提交生产者任务的
      // 所以生产者任务一旦被提交，blockingQueue 里一定是有空余位置的，我们不需要检查队列是否满
      // 如果时间序列对应的 reader 还有数据，进入循环体
      while (reader.hasNextBatch()) {
        BatchData batchData = reader.nextBatch();
        // 由于拿到的 BatchData 有可能为空，所以需要一直迭代到第一个不为空的 BatchData
        if (batchData.isEmpty()) {
          continue;
        }
        // 将不为空的 batchData 放进阻塞队列中，此时的阻塞队列一定是不满的，所以不会阻塞
        blockingQueue.put(batchData);
        // 如果阻塞队列仍然没有满，生产者任务再次向线程池里递归地提交自己，进行下一个 batchData 的获取
        if (blockingQueue.remainingCapacity() > 0) {
          pool.submit(this);
        }
        // 如果阻塞队列满了，生产者任务退出，并将对应 reader 的 managedByQueryManager 置为 false
        else {
          reader.setManagedByQueryManager(false);
        }
        return;
      }
      // 代码执行到这边，代表之前的 while 循环条件不满足，即该时间序列对应的 reader 里没有数据了
      // 我们往阻塞队列里放入一个 SignalBatchData，用以告知消费者，这个时间序列已经没有数据了，不需要再从该时间序列对应的队列里取数据了
      blockingQueue.put(SignalBatchData.getInstance());
      // 将 reader 的 hasRemaining 字段置为 false
      // 通知消费者不需要再为这个时间序列提交生产者任务
      reader.setHasRemaining(false);
      // 将 reader 的 managedByQueryManager 字段置为 false
      reader.setManagedByQueryManager(false);
    }
  } catch (InterruptedException e) {
    LOGGER.error("Interrupted while putting into the blocking queue: ", e);
    Thread.currentThread().interrupt();
  } catch (IOException e) {
    LOGGER.error("Something gets wrong while reading from the series reader: ", e);
  } catch (Exception e) {
    LOGGER.error("Something gets wrong: ", e);
  }
}
```

接下来介绍消费者的代码，消费者的主要逻辑是从每个时间序列的队列里拿出值，做时间戳对齐，然后拼凑结果集。时间戳的对齐主要通过一个时间戳的最小堆来实现，如果该时间序列的时间戳等于堆顶的时间戳，则取出该值，反之，将该时间戳下该时间序列的值置为`null`。

先介绍消费者任务的一些重要字段

* ```
  TimeSelector timeHeap;
  ```

  时间戳的最小堆，用以实现时间戳对齐操作

* ```
  BlockingQueue<BatchData>[] blockingQueueArray;
  ```

  阻塞队列的数组，用以存储每个时间序列对应的阻塞队列

* ```
  boolean[] noMoreDataInQueueArray
  ```

  用以表征某个时间序列的阻塞队列里还有没有值，如果为 false，则消费者不会再去调用`take()`方法，以防消费者线程被阻塞。

* ```
  BatchData[] cachedBatchDataArray
  ```

  缓存从阻塞队列里取出的一个 BatchData，因为阻塞队列里`take()`出的`BatchData`并不能一次性消费完，所以需要做缓存

在消费者`RawQueryDataSetWithoutValueFilter`的构造函数里首先调用了`init()`方法

#### init()

```
private void init() throws InterruptedException {
	timeHeap = new TimeSelector(seriesReaderList.size() << 1, ascending);
	// 为每个时间序列构建生产者任务
	for (int i = 0; i < seriesReaderList.size(); i++) {
	  ManagedSeriesReader reader = seriesReaderList.get(i);
	  reader.setHasRemaining(true);
	  reader.setManagedByQueryManager(true);
	  pool.submit(new ReadTask(reader, blockingQueueArray[i], paths.get(i).getFullPath()));
	}
	// 初始化最小堆，填充每个时间序列对应的缓存
	for (int i = 0; i < seriesReaderList.size(); i++) {
	  // 调用 fillCache(int) 方法填充缓存
	  fillCache(i);
	  // 尝试将每个时间序列的当前最小时间戳放进堆中
	  if (cachedBatchDataArray[i] != null && cachedBatchDataArray[i].hasCurrent()) {
	    long time = cachedBatchDataArray[i].currentTime();
	    timeHeap.add(time);
	  }
	}
}
```

####  fillCache(int)

该方法负责从阻塞队列中取出数据，并填充缓存，具体逻辑见下文注释

```
private void fillCache(int seriesIndex) throws InterruptedException {
    // 从阻塞队列中拿数据，如果没有数据，则会阻塞等待队列中有数据
	BatchData batchData = blockingQueueArray[seriesIndex].take();
	// 如果是一个信号 BatchData，则将相应时间序列的 oMoreDataInQueue 置为 false
	if (batchData instanceof SignalBatchData) {
	  noMoreDataInQueueArray[seriesIndex] = true;
	} else if (batchData instanceof ExceptionBatchData) {
      // 当生产者线程发生异常时释放查询资源
      ExceptionBatchData exceptionBatchData = (ExceptionBatchData) batchData;
      LOGGER.error("exception happened in producer thread", exceptionBatchData.getException());
      if (exceptionBatchData.getException() instanceof IOException) {
        throw (IOException) exceptionBatchData.getException();
      } else if (exceptionBatchData.getException() instanceof RuntimeException) {
        throw (RuntimeException) exceptionBatchData.getException();
      }
	else {
	  // 将取出的 BatchData 放进 cachedBatchDataArray 缓存起来
	  cachedBatchDataArray[seriesIndex] = batchData;
	
	  // 这里加锁的原因与生产者任务那边一样，是为了保证对于 blockingQueue 满不满的判断是正确同步的
	  synchronized (seriesReaderList.get(seriesIndex)) {
	    // 只有当阻塞队列不满的时候，我们才需要判断是不是需要提交生产者任务，这里也保证了生产者任务会被提交，当且仅当阻塞队列不满
	    if (blockingQueueArray[seriesIndex].remainingCapacity() > 0) {
	      ManagedSeriesReader reader = seriesReaderList.get(seriesIndex);、
	      // 如果该时间序列的 reader 并没有被查询管理器管理（即生产者任务由于队列满了，自行退出），并且该 reader 里还有数据，我们需要再次提交该时间序列的生产者任务
	      if (!reader.isManagedByQueryManager() && reader.hasRemaining()) {
	        reader.setManagedByQueryManager(true);
	        pool.submit(new ReadTask(reader, blockingQueueArray[seriesIndex]));
	      }
	    }
	  }
	}
}
```

有了每个时间序列的数据，接下来就是将每个时间戳的数据做对齐，并将结果组装成`TSQueryDataSet`返回。这里的逻辑封装在`fillBuffer()`方法中，该方法里还包含了`limit`和`offset`，以及格式化结果集的逻辑，对此我们不作赘述，只分析其中数据读取和时间戳对齐的流程。

```
// 从最小堆中取出当前时间戳
long minTime = timeHeap.pollFirst();
for (int seriesIndex = 0; seriesIndex < seriesNum; seriesIndex++) {
	if (cachedBatchDataArray[seriesIndex] == null
	    || !cachedBatchDataArray[seriesIndex].hasCurrent()
	    || cachedBatchDataArray[seriesIndex].currentTime() != minTime) {
	  // 该时间序列在当前时间戳没有数据，置为 null
	  ...
	  
	} else {
	  // 该时间序列在当前时间戳有数据，将该数据格式化成结果集格式
	  TSDataType type = cachedBatchDataArray[seriesIndex].getDataType();
	  ...
	  
	}
		
  // 将该时间序列缓存的 batchdata 游标向后移
  cachedBatchDataArray[seriesIndex].next();
	
  // 如果当前缓存的 batchdata 为空，并且阻塞队列依然有数据，则再次调用 fillCache() 方法填充缓存
  if (!cachedBatchDataArray[seriesIndex].hasCurrent()
      && !noMoreDataInQueueArray[seriesIndex]) {
    fillCache(seriesIndex);
  }
	
  // 尝试将该时间序列的下一个时间戳放进最小堆中
  if (cachedBatchDataArray[seriesIndex].hasCurrent()) {
    long time = cachedBatchDataArray[seriesIndex].currentTime();
    timeHeap.add(time);
  }
}
```

## 不包含值过滤条件 + 结果集不按时间戳对齐

### org.apache.iotdb.db.query.dataset.NonAlignEngineDataSet

`NonAlignEngineDataSet`实现了没有值过滤条件，且不需要按照时间戳对齐的查询逻辑。这里的查询逻辑跟`RawQueryDataSetWithoutValueFilter`很类似，但是它的消费者逻辑更为简单，因为不需要做时间戳对齐的操作。并且每个生产者任务中也可以做更多的工作，不仅可以从 Reader 中取出 BatchData，还可以进一步讲取出的 BatchData 格式化为结果集需要的输出，从而提高了程序的并行度。如此，消费者只需要从每个阻塞队列里取出数据，set 进`TSQueryNonAlignDataSet`相应的位置即可。

具体的查询逻辑，在此就不再赘述了，可以参照`RawQueryDataSetWithoutValueFilter`的查询逻辑分析。

## 包含值过滤条件 + 结果集按时间戳对齐

### org.apache.iotdb.db.query.dataset.RawQueryDataSetWithValueFilter

`EngineDataSetWithValueFilter`实现了有值过滤条件的查询逻辑。

它的查询逻辑是，首先根据查询条件生成满足过滤条件的时间戳，通过满足条件的时间戳查询投影列的值，然后返回结果集。它有四个字段

* ```
  TimeGenerator timeGenerator;
  ```

  是用来生成满足过滤条件的时间戳的

* ```
  List<IReaderByTimestamp> seriesReaderByTimestampList;
  ```

  每个时间序列对应的 reader，用来根据时间戳获取数据

* ```
  List<Boolean> cached;
  ```

  当前是否缓存了数据行

* ```
  List<RowRecord> cachedRowRecords
  ```

  当前缓存的数据行

它的主要查询逻辑封装在`cacheRowRecord()`方法中，具体分析见代码中的注释

#### cacheRowRecord()

```
private boolean cacheRowRecord() throws IOException {
   // 判断有没有下一个符合条件的时间戳
	while (timeGenerator.hasNext()) {
	  boolean hasField = false;
	  // 获得当前符合条件的时间戳
	  long timestamp = timeGenerator.next();
	  RowRecord rowRecord = new RowRecord(timestamp);
	  for (int i = 0; i < seriesReaderByTimestampList.size(); i++) {
	    // 根获得每个时间序列当前时间戳下的 value
	    IReaderByTimestamp reader = seriesReaderByTimestampList.get(i);
	    Object value = reader.getValueInTimestamp(timestamp);
	    // 如果该时间序列在当前时间戳下没有值，则置 null
	    if (value == null) {
	      rowRecord.addField(null);
	    } 
	    // 否则将 hasField 置为 true
	    else {
	      hasField = true;
	      rowRecord.addField(value, dataTypes.get(i));
	    }
	  }
	  // 如果该时间戳下，任何一个时间序列有值，则表示该时间戳有效，缓存该数据行
	  if (hasField) {
	    hasCachedRowRecord = true;
	    cachedRowRecord = rowRecord;
	    break;
	  }
	}
	return hasCachedRowRecord;
}
```