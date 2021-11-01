/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.query.dataset;

import org.apache.iotdb.db.concurrent.WrappedRunnable;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.metadata.VectorPartialPath;
import org.apache.iotdb.db.query.control.QueryTimeManager;
import org.apache.iotdb.db.query.pool.QueryTaskPoolManager;
import org.apache.iotdb.db.query.reader.series.ManagedSeriesReader;
import org.apache.iotdb.db.tools.watermark.WatermarkEncoder;
import org.apache.iotdb.db.utils.datastructure.TimeSelector;
import org.apache.iotdb.service.rpc.thrift.TSQueryDataSet;
import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.common.ExceptionBatchData;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.read.common.SignalBatchData;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;
import org.apache.iotdb.tsfile.utils.BytesUtils;
import org.apache.iotdb.tsfile.utils.PublicBAOS;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;
import org.apache.iotdb.tsfile.utils.TsPrimitiveType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class RawQueryDataSetWithoutValueFilter extends QueryDataSet
    implements DirectAlignByTimeDataSet, UDFInputDataSet {

  private class ReadTask extends WrappedRunnable {

    private final ManagedSeriesReader reader;
    private final String pathName;
    private final BlockingQueue<BatchData> blockingQueue;

    public ReadTask(
        ManagedSeriesReader reader, BlockingQueue<BatchData> blockingQueue, String pathName) {
      this.reader = reader;
      this.blockingQueue = blockingQueue;
      this.pathName = pathName;
    }

    @Override
    public void runMayThrow() {
      try {
        // check the status of mainThread before next reading
        // 1. Main thread quits because of timeout
        // 2. Main thread quits because of getting enough fetchSize result
        if (!QueryTimeManager.checkQueryAlive(queryId)) {
          return;
        }

        synchronized (reader) {
          // if the task is submitted, there must be free space in the queue
          // so here we don't need to check whether the queue has free space
          // the reader has next batch
          while (reader.hasNextBatch()) {
            BatchData batchData = reader.nextBatch();
            // iterate until we get first batch data with valid value
            if (batchData.isEmpty()) {
              continue;
            }
            blockingQueue.put(batchData);
            // if the queue also has free space, just submit another itself
            if (blockingQueue.remainingCapacity() > 0) {
              TASK_POOL_MANAGER.submit(this);
            }
            // the queue has no more space
            // remove itself from the QueryTaskPoolManager
            else {
              reader.setManagedByQueryManager(false);
            }
            return;
          }
          // there are no batch data left in this reader
          // put the signal batch data into queue
          blockingQueue.put(SignalBatchData.getInstance());
          // set the hasRemaining field in reader to false
          // tell the Consumer not to submit another task for this reader any more
          reader.setHasRemaining(false);
          // remove itself from the QueryTaskPoolManager
          reader.setManagedByQueryManager(false);
        }
      } catch (InterruptedException e) {
        LOGGER.error("Interrupted while putting into the blocking queue: ", e);
        Thread.currentThread().interrupt();
        reader.setHasRemaining(false);
      } catch (IOException e) {
        putExceptionBatchData(
            e,
            String.format(
                "Something gets wrong while reading from the series reader %s: ", pathName));
      } catch (Exception e) {
        putExceptionBatchData(e, "Something gets wrong: ");
      }
    }

    private void putExceptionBatchData(Exception e, String logMessage) {
      try {
        LOGGER.error(logMessage, e);
        reader.setHasRemaining(false);
        blockingQueue.put(new ExceptionBatchData(e));
      } catch (InterruptedException ex) {
        LOGGER.error("Interrupted while putting ExceptionBatchData into the blocking queue: ", ex);
        Thread.currentThread().interrupt();
      }
    }
  }

  protected List<ManagedSeriesReader> seriesReaderList;

  protected TimeSelector timeHeap;

  // Blocking queue list for each batch reader
  private final BlockingQueue<BatchData>[] blockingQueueArray;

  // indicate that there is no more batch data in the corresponding queue
  // in case that the consumer thread is blocked on the queue and won't get runnable any more
  // this field is not same as the `hasRemaining` in SeriesReaderWithoutValueFilter
  // even though the `hasRemaining` in SeriesReaderWithoutValueFilter is false
  // noMoreDataInQueue can still be true
  // its usage is to tell the consumer thread not to call the take() method.
  protected boolean[] noMoreDataInQueueArray;

  protected BatchData[] cachedBatchDataArray;

  private int bufferNum;

  // capacity for blocking queue
  private static final int BLOCKING_QUEUE_CAPACITY = 5;

  private final long queryId;

  private static final QueryTaskPoolManager TASK_POOL_MANAGER = QueryTaskPoolManager.getInstance();

  private static final Logger LOGGER =
      LoggerFactory.getLogger(RawQueryDataSetWithoutValueFilter.class);

  /**
   * constructor of EngineDataSetWithoutValueFilter.
   *
   * @param paths paths in List structure
   * @param dataTypes time series data type
   * @param readers readers in List(IPointReader) structure
   */
  public RawQueryDataSetWithoutValueFilter(
      long queryId,
      List<PartialPath> paths,
      List<TSDataType> dataTypes,
      List<ManagedSeriesReader> readers,
      boolean ascending)
      throws IOException, InterruptedException {
    super(new ArrayList<>(paths), dataTypes, ascending);
    this.queryId = queryId;
    this.seriesReaderList = readers;
    blockingQueueArray = new BlockingQueue[readers.size()];
    for (int i = 0; i < seriesReaderList.size(); i++) {
      blockingQueueArray[i] = new LinkedBlockingQueue<>(BLOCKING_QUEUE_CAPACITY);
    }
    cachedBatchDataArray = new BatchData[readers.size()];
    noMoreDataInQueueArray = new boolean[readers.size()];
    bufferNum = 0;
    for (PartialPath path : paths) {
      if (path instanceof VectorPartialPath) {
        bufferNum += ((VectorPartialPath) path).getSubSensorsList().size();
      } else {
        bufferNum += 1;
      }
    }
    init();
  }

  /**
   * Dummy dataSet for redirect query.
   *
   * @param queryId queryId for the query.
   */
  public RawQueryDataSetWithoutValueFilter(long queryId) {
    this.queryId = queryId;
    blockingQueueArray = new BlockingQueue[0];
    timeHeap = new TimeSelector(0, ascending);
  }

  private void init() throws IOException, InterruptedException {
    timeHeap = new TimeSelector(seriesReaderList.size() << 1, ascending);
    for (int i = 0; i < seriesReaderList.size(); i++) {
      ManagedSeriesReader reader = seriesReaderList.get(i);
      reader.setHasRemaining(true);
      reader.setManagedByQueryManager(true);
      TASK_POOL_MANAGER.submit(
          new ReadTask(reader, blockingQueueArray[i], paths.get(i).getFullPath()));
    }
    for (int i = 0; i < seriesReaderList.size(); i++) {
      // check the interrupted status of query before taking next batch
      QueryTimeManager.checkQueryAlive(queryId);
      fillCache(i);
      // try to put the next timestamp into the heap
      if (cachedBatchDataArray[i] != null && cachedBatchDataArray[i].hasCurrent()) {
        long time = cachedBatchDataArray[i].currentTime();
        timeHeap.add(time);
      }
    }
  }

  /**
   * for RPC in RawData query between client and server fill time buffer, value buffers and bitmap
   * buffers
   */
  @SuppressWarnings("squid:S3776") // Suppress high Cognitive Complexity warning
  @Override
  public TSQueryDataSet fillBuffer(int fetchSize, WatermarkEncoder encoder)
      throws IOException, InterruptedException {
    int seriesNum = seriesReaderList.size();
    TSQueryDataSet tsQueryDataSet = new TSQueryDataSet();

    PublicBAOS timeBAOS = new PublicBAOS();

    PublicBAOS[] valueBAOSList = new PublicBAOS[bufferNum];
    PublicBAOS[] bitmapBAOSList = new PublicBAOS[bufferNum];

    for (int bufferIndex = 0; bufferIndex < bufferNum; bufferIndex++) {
      valueBAOSList[bufferIndex] = new PublicBAOS();
      bitmapBAOSList[bufferIndex] = new PublicBAOS();
    }

    // used to record a bitmap for every 8 row records
    int[] currentBitmapList = new int[bufferNum];
    int rowCount = 0;
    while (rowCount < fetchSize) {

      if ((rowLimit > 0 && alreadyReturnedRowNum >= rowLimit) || timeHeap.isEmpty()) {
        break;
      }

      long minTime = timeHeap.pollFirst();

      if (withoutAnyNull && filterRowRecord(seriesNum, minTime)) {
        continue;
      }

      if (rowOffset == 0) {
        timeBAOS.write(BytesUtils.longToBytes(minTime));
      }

      for (int seriesIndex = 0, bufferIndex = 0; seriesIndex < seriesNum; seriesIndex++) {

        if (cachedBatchDataArray[seriesIndex] == null
            || !cachedBatchDataArray[seriesIndex].hasCurrent()
            || cachedBatchDataArray[seriesIndex].currentTime() != minTime) {
          // current batch is empty or does not have value at minTime
          if (rowOffset == 0) {
            if (paths.get(seriesIndex) instanceof VectorPartialPath) {
              for (int i = 0;
                  i < ((VectorPartialPath) paths.get(seriesIndex)).getSubSensorsList().size();
                  i++) {
                currentBitmapList[bufferIndex] = (currentBitmapList[bufferIndex] << 1);
                bufferIndex++;
              }
            } else {
              currentBitmapList[bufferIndex] = (currentBitmapList[bufferIndex] << 1);
              bufferIndex++;
            }
          }
        } else {
          // current batch has value at minTime, consume current value
          if (rowOffset == 0) {
            TSDataType type = cachedBatchDataArray[seriesIndex].getDataType();
            switch (type) {
              case INT32:
                currentBitmapList[bufferIndex] = (currentBitmapList[bufferIndex] << 1) | FLAG;
                int intValue = cachedBatchDataArray[seriesIndex].getInt();
                if (encoder != null && encoder.needEncode(minTime)) {
                  intValue = encoder.encodeInt(intValue, minTime);
                }
                ReadWriteIOUtils.write(intValue, valueBAOSList[seriesIndex]);
                bufferIndex++;
                break;
              case INT64:
                currentBitmapList[bufferIndex] = (currentBitmapList[bufferIndex] << 1) | FLAG;
                long longValue = cachedBatchDataArray[seriesIndex].getLong();
                if (encoder != null && encoder.needEncode(minTime)) {
                  longValue = encoder.encodeLong(longValue, minTime);
                }
                ReadWriteIOUtils.write(longValue, valueBAOSList[seriesIndex]);
                bufferIndex++;
                break;
              case FLOAT:
                currentBitmapList[bufferIndex] = (currentBitmapList[bufferIndex] << 1) | FLAG;
                float floatValue = cachedBatchDataArray[seriesIndex].getFloat();
                if (encoder != null && encoder.needEncode(minTime)) {
                  floatValue = encoder.encodeFloat(floatValue, minTime);
                }
                ReadWriteIOUtils.write(floatValue, valueBAOSList[seriesIndex]);
                bufferIndex++;
                break;
              case DOUBLE:
                currentBitmapList[bufferIndex] = (currentBitmapList[bufferIndex] << 1) | FLAG;
                double doubleValue = cachedBatchDataArray[seriesIndex].getDouble();
                if (encoder != null && encoder.needEncode(minTime)) {
                  doubleValue = encoder.encodeDouble(doubleValue, minTime);
                }
                ReadWriteIOUtils.write(doubleValue, valueBAOSList[seriesIndex]);
                bufferIndex++;
                break;
              case BOOLEAN:
                currentBitmapList[bufferIndex] = (currentBitmapList[bufferIndex] << 1) | FLAG;
                ReadWriteIOUtils.write(
                    cachedBatchDataArray[seriesIndex].getBoolean(), valueBAOSList[seriesIndex]);
                bufferIndex++;
                break;
              case TEXT:
                currentBitmapList[bufferIndex] = (currentBitmapList[bufferIndex] << 1) | FLAG;
                ReadWriteIOUtils.write(
                    cachedBatchDataArray[seriesIndex].getBinary(), valueBAOSList[seriesIndex]);
                bufferIndex++;
                break;
              case VECTOR:
                for (TsPrimitiveType primitiveVal : cachedBatchDataArray[seriesIndex].getVector()) {
                  if (primitiveVal == null) {
                    currentBitmapList[bufferIndex] = (currentBitmapList[bufferIndex] << 1);
                    bufferIndex++;
                    continue;
                  }
                  currentBitmapList[bufferIndex] = (currentBitmapList[bufferIndex] << 1) | FLAG;
                  switch (primitiveVal.getDataType()) {
                    case INT32:
                      int intVal = primitiveVal.getInt();
                      if (encoder != null && encoder.needEncode(minTime)) {
                        intVal = encoder.encodeInt(intVal, minTime);
                      }
                      ReadWriteIOUtils.write(intVal, valueBAOSList[bufferIndex]);
                      break;
                    case INT64:
                      long longVal = primitiveVal.getLong();
                      if (encoder != null && encoder.needEncode(minTime)) {
                        longVal = encoder.encodeLong(longVal, minTime);
                      }
                      ReadWriteIOUtils.write(longVal, valueBAOSList[bufferIndex]);
                      break;
                    case FLOAT:
                      float floatVal = primitiveVal.getFloat();
                      if (encoder != null && encoder.needEncode(minTime)) {
                        floatVal = encoder.encodeFloat(floatVal, minTime);
                      }
                      ReadWriteIOUtils.write(floatVal, valueBAOSList[bufferIndex]);
                      break;
                    case DOUBLE:
                      double doubleVal = primitiveVal.getDouble();
                      if (encoder != null && encoder.needEncode(minTime)) {
                        doubleVal = encoder.encodeDouble(doubleVal, minTime);
                      }
                      ReadWriteIOUtils.write(doubleVal, valueBAOSList[bufferIndex]);
                      break;
                    case BOOLEAN:
                      ReadWriteIOUtils.write(primitiveVal.getBoolean(), valueBAOSList[bufferIndex]);
                      break;
                    case TEXT:
                      ReadWriteIOUtils.write(primitiveVal.getBinary(), valueBAOSList[bufferIndex]);
                      break;
                    default:
                      throw new UnSupportedDataTypeException(
                          String.format("Data type %s is not supported.", type));
                  }
                  bufferIndex++;
                }
                break;
              default:
                throw new UnSupportedDataTypeException(
                    String.format("Data type %s is not supported.", type));
            }
          }

          prepareForNext(seriesIndex);
        }
      }

      if (rowOffset == 0) {
        rowCount++;
        if (rowCount % 8 == 0) {
          for (int bufferIndex = 0; bufferIndex < bufferNum; bufferIndex++) {
            ReadWriteIOUtils.write(
                (byte) currentBitmapList[bufferIndex], bitmapBAOSList[bufferIndex]);
            // we should clear the bitmap every 8 row record
            currentBitmapList[bufferIndex] = 0;
          }
        }
        if (rowLimit > 0) {
          alreadyReturnedRowNum++;
        }
      } else {
        rowOffset--;
      }
    }

    /*
     * feed the bitmap with remaining 0 in the right
     * if current bitmap is 00011111 and remaining is 3, after feeding the bitmap is 11111000
     */
    if (rowCount > 0) {
      int remaining = rowCount % 8;
      if (remaining != 0) {
        for (int bufferIndex = 0; bufferIndex < bufferNum; bufferIndex++) {
          ReadWriteIOUtils.write(
              (byte) (currentBitmapList[bufferIndex] << (8 - remaining)),
              bitmapBAOSList[bufferIndex]);
        }
      }
    }

    // set time buffer
    ByteBuffer timeBuffer = ByteBuffer.allocate(timeBAOS.size());
    timeBuffer.put(timeBAOS.getBuf(), 0, timeBAOS.size());
    timeBuffer.flip();
    tsQueryDataSet.setTime(timeBuffer);

    List<ByteBuffer> valueBufferList = new ArrayList<>();
    List<ByteBuffer> bitmapBufferList = new ArrayList<>();

    for (int bufferIndex = 0; bufferIndex < bufferNum; bufferIndex++) {

      // add value buffer of current series
      putPBOSToBuffer(valueBAOSList, valueBufferList, bufferIndex);

      // add bitmap buffer of current series
      putPBOSToBuffer(bitmapBAOSList, bitmapBufferList, bufferIndex);
    }

    // set value buffers and bitmap buffers
    tsQueryDataSet.setValueList(valueBufferList);
    tsQueryDataSet.setBitmapList(bitmapBufferList);

    return tsQueryDataSet;
  }

  /** if any column in the row record is null, we filter it. */
  private boolean filterRowRecord(int seriesNum, long minTime)
      throws IOException, InterruptedException {
    boolean hasNull = false;
    for (int seriesIndex = 0; seriesIndex < seriesNum; seriesIndex++) {
      if (cachedBatchDataArray[seriesIndex] == null
          || !cachedBatchDataArray[seriesIndex].hasCurrent()
          || cachedBatchDataArray[seriesIndex].currentTime() != minTime) {
        hasNull = true;
      } else {
        if (TSDataType.VECTOR == cachedBatchDataArray[seriesIndex].getDataType()) {
          for (TsPrimitiveType primitiveVal : cachedBatchDataArray[seriesIndex].getVector()) {
            if (primitiveVal == null) {
              hasNull = true;
              break;
            }
          }
        }
      }
      if (hasNull) {
        break;
      }
    }
    if (hasNull) {
      for (int seriesIndex = 0; seriesIndex < seriesNum; seriesIndex++) {
        if (cachedBatchDataArray[seriesIndex] != null
            && cachedBatchDataArray[seriesIndex].hasCurrent()
            && cachedBatchDataArray[seriesIndex].currentTime() == minTime) {
          prepareForNext(seriesIndex);
        }
      }
      return true;
    }
    return false;
  }

  private void prepareForNext(int seriesIndex) throws IOException, InterruptedException {
    // move next
    cachedBatchDataArray[seriesIndex].next();

    // get next batch if current batch is empty and still have remaining batch data in queue
    if (!cachedBatchDataArray[seriesIndex].hasCurrent() && !noMoreDataInQueueArray[seriesIndex]) {
      fillCache(seriesIndex);
    }

    // try to put the next timestamp into the heap
    if (cachedBatchDataArray[seriesIndex].hasCurrent()) {
      timeHeap.add(cachedBatchDataArray[seriesIndex].currentTime());
    }
  }

  protected void fillCache(int seriesIndex) throws IOException, InterruptedException {
    BatchData batchData = blockingQueueArray[seriesIndex].take();
    // no more batch data in this time series queue
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

    } else { // there are more batch data in this time series queue
      cachedBatchDataArray[seriesIndex] = batchData;

      synchronized (seriesReaderList.get(seriesIndex)) {
        // we only need to judge whether to submit another task when the queue is not full
        if (blockingQueueArray[seriesIndex].remainingCapacity() > 0) {
          ManagedSeriesReader reader = seriesReaderList.get(seriesIndex);
          // if the reader isn't being managed and still has more data,
          // that means this read task leave the pool before because the queue has no more space
          // now we should submit it again
          if (!reader.isManagedByQueryManager() && reader.hasRemaining()) {
            reader.setManagedByQueryManager(true);
            TASK_POOL_MANAGER.submit(
                new ReadTask(
                    reader, blockingQueueArray[seriesIndex], paths.get(seriesIndex).getFullPath()));
          }
        }
      }
    }
  }

  private void putPBOSToBuffer(
      PublicBAOS[] bitmapBAOSList, List<ByteBuffer> bitmapBufferList, int tsIndex) {
    ByteBuffer bitmapBuffer = ByteBuffer.allocate(bitmapBAOSList[tsIndex].size());
    bitmapBuffer.put(bitmapBAOSList[tsIndex].getBuf(), 0, bitmapBAOSList[tsIndex].size());
    bitmapBuffer.flip();
    bitmapBufferList.add(bitmapBuffer);
  }

  /** for spark/hadoop/hive integration and test */
  @Override
  public boolean hasNextWithoutConstraint() {
    return !timeHeap.isEmpty();
  }

  /** for spark/hadoop/hive integration and test */
  @SuppressWarnings("squid:S3776") // Suppress high Cognitive Complexity warning
  @Override
  public RowRecord nextWithoutConstraint() throws IOException {
    long minTime = timeHeap.pollFirst();
    RowRecord record = new RowRecord(minTime);

    int seriesNumber = seriesReaderList.size();
    for (int seriesIndex = 0; seriesIndex < seriesNumber; seriesIndex++) {
      if (cachedBatchDataArray[seriesIndex] == null
          || !cachedBatchDataArray[seriesIndex].hasCurrent()
          || cachedBatchDataArray[seriesIndex].currentTime() != minTime) {
        if (paths.get(seriesIndex) instanceof VectorPartialPath) {
          for (int i = 0;
              i < ((VectorPartialPath) paths.get(seriesIndex)).getSubSensorsList().size();
              i++) {
            record.addField(null);
          }
        } else {
          record.addField(null);
        }
      } else {
        TSDataType dataType = dataTypes.get(seriesIndex);
        if (dataType == TSDataType.VECTOR) {
          for (TsPrimitiveType primitiveVal : cachedBatchDataArray[seriesIndex].getVector()) {
            if (primitiveVal == null) {
              record.addField(null);
            } else {
              record.addField(primitiveVal.getValue(), primitiveVal.getDataType());
            }
          }
        } else {
          record.addField(cachedBatchDataArray[seriesIndex].currentValue(), dataType);
        }
        cacheNext(seriesIndex);
      }
    }

    return record;
  }

  @Override
  public boolean hasNextRowInObjects() {
    return !timeHeap.isEmpty();
  }

  @Override
  public Object[] nextRowInObjects() throws IOException {
    int seriesNumber = seriesReaderList.size();

    long minTime = timeHeap.pollFirst();
    Object[] rowInObjects = new Object[seriesNumber + 1];
    rowInObjects[seriesNumber] = minTime;

    for (int seriesIndex = 0; seriesIndex < seriesNumber; seriesIndex++) {
      if (cachedBatchDataArray[seriesIndex] != null
          && cachedBatchDataArray[seriesIndex].hasCurrent()
          && cachedBatchDataArray[seriesIndex].currentTime() == minTime) {
        rowInObjects[seriesIndex] = cachedBatchDataArray[seriesIndex].currentValue();
        cacheNext(seriesIndex);
      }
    }

    return rowInObjects;
  }

  private void cacheNext(int seriesIndex) throws IOException {
    // move next
    cachedBatchDataArray[seriesIndex].next();

    // check the interrupted status of query before taking next batch
    QueryTimeManager.checkQueryAlive(queryId);

    // get next batch if current batch is empty and still have remaining batch data in queue
    if (!cachedBatchDataArray[seriesIndex].hasCurrent() && !noMoreDataInQueueArray[seriesIndex]) {
      try {
        fillCache(seriesIndex);
      } catch (InterruptedException e) {
        LOGGER.error("Interrupted while taking from the blocking queue: ", e);
        Thread.currentThread().interrupt();
      } catch (IOException e) {
        LOGGER.error("Got IOException", e);
        throw e;
      }
    }

    // try to put the next timestamp into the heap
    if (cachedBatchDataArray[seriesIndex].hasCurrent()) {
      timeHeap.add(cachedBatchDataArray[seriesIndex].currentTime());
    }
  }
}
