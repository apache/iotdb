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

# Raw data query

## Design principle

The raw data query can be divided into two categories based on whether it contains value filtering conditions.  When no value filter is included, it can be divided into two categories based on the result set structure.

* No value filter (no filter or only time filter)
	* Result set aligned by timestamp (default raw data query)
	* The result set is not aligned with the timestamp (disable align)
* Include value filters
	* Result set aligned by timestamp

The above three queries correspond to three different DataSets in the code and encapsulate the execution logic of these three queries.

## No value filter + result set aligned by timestamp

### org.apache.iotdb.db.query.dataset.RawQueryDataSetWithoutValueFilter

`RawQueryDataSetWithoutValueFilter` implements query logic that has no value filtering conditions and needs to be aligned according to the timestamp.  Although the final query results require that each time series be aligned according to the timestamp, each time series query can be parallelized.  Here, with the idea of a consumer-producer queue, the operation of obtaining data for each time series is decoupled from the operation of finally aligning all time series.  Each time series corresponds to a producer thread and has its corresponding BlockingQueue. The producer task is responsible for reading the corresponding time series data and putting it into the BlockingQueue; there is only one consumer thread responsible for taking out from each time series BlockingQueue.  After the data is aligned with the timestamp, the results are assembled into a TSQueryDataSet and returned.

In specific implementation, considering the resource constraints of the machine, instead of creating a thread for each time series of each query, the thread pool technology is used to submit each time series producer task as a Runnable task  To the thread pool for execution.

The following introduces the producer's code first. It is encapsulated in an internal class ReadTask which is a RawQueryDataSetWithoutValueFilter and implements the Runnable interface.

### org.apache.iotdb.db.query.dataset.RawQueryDataSetWithoutValueFilter.ReadTask

`ReadTask` has two fields

* private final ManagedSeriesReader reader;
* private BlockingQueue\<BatchData\> blockingQueue;

The `ManagedSeriesReader` interface inherits the IBatchReader interface, which is mainly used to read data from a single time series, and adds the following four methods

```
boolean isManagedByQueryManager();

void setManagedByQueryManager(boolean managedByQueryManager);

boolean hasRemaining();

void setHasRemaining(boolean hasRemaining);
```

The first two methods are used to characterize whether the producer task corresponding to the time series is managed by the query manager, that is, has the producer task exited by itself because the blocking queue is full (the following will explain why it does not block waiting, and  Exit directly); the latter two methods are used to characterize whether there is any data in the reader corresponding to the time series.

`blockingQueue` is the blocking queue of the producer task. In fact, the blocking queue will only block unilaterally when the consumer fetches the data. When the producer puts the data, if the queue is full, it will exit directly without blocking.

Let ’s take a look at the `run ()` method of `ReadTask`. The explanation of the execution process is shown in the code in the form of comments.

#### run()

```
public void run() {
  try {
    // The reason for the lock here is to ensure that the judgment of the fullness of the blockingQueue is correctly synchronized.
    synchronized (reader) {
      // Because every time a producer task is submitted (whether the producer submits itself recursively or the consumer finds that the producer task exits and submits itself), the queue is checked to see if it is full.  Producer task
      // So once the producer task is submitted, there must be a free space in the blockingQueue, we do not need to check whether the queue is full
      // If the reader corresponding to the time series still has data, enter the loop body
      while (reader.hasNextBatch()) {
        BatchData batchData = reader.nextBatch();
        // Since the BatchData obtained may be empty, it needs to iterate to the first BatchData that is not empty
        if (batchData.isEmpty()) {
          continue;
        }
        // Put the non-empty batchData into the blocking queue. At this time, the blocking queue must be dissatisfied, so it will not block.
        blockingQueue.put(batchData);
        // If the blocking queue is still not full, the producer task recursively submits itself to the thread pool for the next batchData
        if (blockingQueue.remainingCapacity() > 0) {
          pool.submit(this);
        }
        // If the blocking queue is full, the producer task exits and the managedByQueryManager corresponding to the reader is set to false
        else {
          reader.setManagedByQueryManager(false);
        }
        return;
      }
      // The code is executed here, which means that the previous while loop condition is not satisfied, that is, there is no data in the reader corresponding to the time series.
      // We put a SignalBatchData in the blocking queue to inform consumers that there is no more data in this time series, and there is no need to fetch data from the queue corresponding to this time series
      blockingQueue.put(SignalBatchData.getInstance());
      // Set the reader's hasRemaining field to false
      // Inform consumers that they no longer need to submit producer tasks for this time series
      reader.setHasRemaining(false);
      // Set the reader's managedByQueryManager field to false
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

Next, introduce the code of the consumer. The main logic of the consumer is to take the value from the queue of each time series, align the timestamps, and then piece together the result set.  The alignment of timestamps is mainly achieved through a minimum heap of timestamps. If the timestamp of the time series is equal to the timestamp at the top of the heap, then the value is taken out; otherwise, the time series value under the timestamp is set to `null  `.

First introduce some important fields of consumer tasks

* TimeSelector timeHeap;

  The smallest heap of timestamps for timestamp alignment

* BlockingQueue\<BatchData\>[] blockingQueueArray;

  An array of blocking queues to store the blocking queues corresponding to each time series

* boolean[] noMoreDataInQueueArray

  There is no value in the blocking queue to represent a certain time series. If it is false, the consumer will not call the `take ()` method to prevent the consumer thread from being blocked.
  
* BatchData[] cachedBatchDataArray

  Cache a BatchData fetched from the blocking queue, because the `BatchData` from` take () `in the blocking queue cannot be consumed all at once, so you need to cache
  

The `init ()` method was first called in the constructor of the consumer `RawQueryDataSetWithoutValueFilter`

#### init()

```
private void init() throws InterruptedException {
	timeHeap = new TimeSelector(seriesReaderList.size() << 1, ascending);
	// Build producer tasks for each time series
	for (int i = 0; i < seriesReaderList.size(); i++) {
	  ManagedSeriesReader reader = seriesReaderList.get(i);
	  reader.setHasRemaining(true);
	  reader.setManagedByQueryManager(true);
	  pool.submit(new ReadTask(reader, blockingQueueArray[i], paths.get(i).getFullPath()));
	}
	// Initialize the minimum heap and fill the cache for each time series
	for (int i = 0; i < seriesReaderList.size(); i++) {
	  // Call fillCache (int) method to fill the cache
	  fillCache(i);
	  // Try to put the current minimum timestamp of each time series into the heap
	  if (cachedBatchDataArray[i] != null && cachedBatchDataArray[i].hasCurrent()) {
	    long time = cachedBatchDataArray[i].currentTime();
	    timeHeap.add(time);
	  }
	}
}
```

####  fillCache(int)

This method is responsible for fetching data from the blocking queue and filling the cache. For the specific logic, see the note below.

```
private void fillCache(int seriesIndex) throws InterruptedException {
    // Get data from the blocking queue, if there is no data, it will block waiting for data in the queue
	BatchData batchData = blockingQueueArray[seriesIndex].take();
	// If it is a signal BatchData, set oMoreDataInQueue of the corresponding time series to false
	if (batchData instanceof SignalBatchData) {
	  noMoreDataInQueueArray[seriesIndex] = true;
	} else if (batchData instanceof ExceptionBatchData) {
      // exception happened in producer thread
      ExceptionBatchData exceptionBatchData = (ExceptionBatchData) batchData;
      LOGGER.error("exception happened in producer thread", exceptionBatchData.getException());
      if (exceptionBatchData.getException() instanceof IOException) {
        throw (IOException) exceptionBatchData.getException();
      } else if (exceptionBatchData.getException() instanceof RuntimeException) {
        throw (RuntimeException) exceptionBatchData.getException();
      }
	else {
	  // Cache the retrieved BatchData into cachedBatchDataArray
	  cachedBatchDataArray[seriesIndex] = batchData;
	
	  // The reason for locking here is the same as that of the producer task, in order to ensure that the judgment of the fullness of the blockingQueue is correctly synchronized.
	  synchronized (seriesReaderList.get(seriesIndex)) {
	    // Only when the blocking queue is not full, do we need to determine whether it is necessary to submit the producer task. This also guarantees that the producer task will be submitted if and only if the blocking queue is not full.
	    if (blockingQueueArray[seriesIndex].remainingCapacity() > 0) {
	      ManagedSeriesReader reader = seriesReaderList.get(seriesIndex);、
	      // If the reader of the time series is not managed by the query manager (that is, the producer task exits because the queue is full), and there is still data in the reader, we need to submit the producer task of the time series again
	      if (!reader.isManagedByQueryManager() && reader.hasRemaining()) {
	        reader.setManagedByQueryManager(true);
	        pool.submit(new ReadTask(reader, blockingQueueArray[seriesIndex]));
	      }
	    }
	  }
	}
}
```

With the data for each time series, the next step is to align the data for each time stamp and assemble the results into a TSQueryDataSet to return.  The logic here is encapsulated in the fillBuffer () method. This method also contains the logic of limit and offset, and the format of the result set. We will not go into details here, but only analyze the process of data reading and time stamp alignment.

```
// Fetch the current timestamp from the smallest heap
long minTime = timeHeap.pollFirst();
for (int seriesIndex = 0; seriesIndex < seriesNum; seriesIndex++) {
	if (cachedBatchDataArray[seriesIndex] == null
	    || !cachedBatchDataArray[seriesIndex].hasCurrent()
	    || cachedBatchDataArray[seriesIndex].currentTime() != minTime) {
	  // The time series has no data at the current timestamp and is set to null
	  ...
	  
	} else {
	  // The time series has data at the current timestamp, and the data is formatted into a result set format
	  TSDataType type = cachedBatchDataArray[seriesIndex].getDataType();
	  ...
	  
	}
		
  // Move the batchdata cursor of this time series buffer back
  cachedBatchDataArray[seriesIndex].next();
	
  // If the currently cached batchdata is empty and the blocking queue still has data, call the fillCache () method again to fill the cache
  if (!cachedBatchDataArray[seriesIndex].hasCurrent()
      && !noMoreDataInQueueArray[seriesIndex]) {
    fillCache(seriesIndex);
  }
	
  // Try to put the next timestamp of that time series into the smallest heap
  if (cachedBatchDataArray[seriesIndex].hasCurrent()) {
    long time = cachedBatchDataArray[seriesIndex].currentTime();
    timeHeap.add(time);
  }
}
```

## No value filter + result set is not aligned by timestamp

### org.apache.iotdb.db.query.dataset.NonAlignEngineDataSet

`NonAlignEngineDataSet` implements query logic that has no value filtering conditions and does not need to be aligned by timestamp.  The query logic here is similar to RawQueryDataSetWithoutValueFilter, but its consumer logic is simpler, because it does not need to perform timestamp alignment.  And each producer task can also do more work, not only can take out BatchData from Reader, but can further say that the taken out BatchData is formatted into the output required by the result set, thereby improving the parallelism of the program.  In this way, the consumer only needs to fetch data from each blocking queue and set it into the corresponding position of TSQueryNonAlignDataSet.

The specific query logic is not repeated here. You can refer to the query logic analysis of RawQueryDataSetWithoutValueFilter.

## Include value filter + result set aligned by timestamp

### org.apache.iotdb.db.query.dataset.RawQueryDataSetWithValueFilter

`EngineDataSetWithValueFilter` implements query logic with value filter conditions.

Its query logic is to first generate a timestamp that meets the filtering conditions according to the query conditions, query the value of the projection column by the timestamp that meets the conditions, and then return the result set.  It has four fields

* TimeGenerator timeGenerator;

  Is used to generate timestamps that satisfy the filter
  
* List\<IReaderByTimestamp\> seriesReaderByTimestampList;

  Reader for each time series, used to get data based on timestamp

* List<Boolean> cached;

  Whether data rows are currently cached
  
* List<RowRecord> cachedRowRecords

  Data lines currently cached
  

Its main query logic is encapsulated in the `cacheRowRecord ()` method. For detailed analysis, see the comments in the code.

#### cacheRowRecord()

```
private boolean cacheRowRecord() throws IOException {
   // Determine if there is a next eligible timestamp
	while (timeGenerator.hasNext()) {
	  boolean hasField = false;
	  // Get the current eligible timestamp
	  long timestamp = timeGenerator.next();
	  RowRecord rowRecord = new RowRecord(timestamp);
	  for (int i = 0; i < seriesReaderByTimestampList.size(); i++) {
	    // Root to get the value under the current timestamp of each time series
	    IReaderByTimestamp reader = seriesReaderByTimestampList.get(i);
	    Object value = reader.getValueInTimestamp(timestamp);
	    // Null if the time series has no value under the current timestamp
	    if (value == null) {
	      rowRecord.addField(null);
	    } 
	    // Otherwise set hasField to true
	    else {
	      hasField = true;
	      rowRecord.addField(value, dataTypes.get(i));
	    }
	  }
	  // If there is a value in any time series under the timestamp, it means that the timestamp is valid, and the data line is cached
	  if (hasField) {
	    hasCachedRowRecord = true;
	    cachedRowRecord = rowRecord;
	    break;
	  }
	}
	return hasCachedRowRecord;
}
```