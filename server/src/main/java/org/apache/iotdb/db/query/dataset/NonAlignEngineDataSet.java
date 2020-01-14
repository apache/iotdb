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

import org.apache.iotdb.db.query.pool.QueryTaskPoolManager;
import org.apache.iotdb.db.query.reader.ManagedSeriesReader;
import org.apache.iotdb.db.tools.watermark.WatermarkEncoder;
import org.apache.iotdb.service.rpc.thrift.TSQueryNonAlignDataSet;
import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.utils.PublicBAOS;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class NonAlignEngineDataSet extends QueryDataSet {
  
  private static class ReadTask implements Runnable {
    
    private final ManagedSeriesReader reader;
    private BlockingQueue<Pair<ByteBuffer, ByteBuffer>> blockingQueue;
    private WatermarkEncoder encoder;
    NonAlignEngineDataSet dataSet;
    private int index;

    
    public ReadTask(ManagedSeriesReader reader, 
        BlockingQueue<Pair<ByteBuffer, ByteBuffer>> blockingQueue,
        WatermarkEncoder encoder, NonAlignEngineDataSet dataSet, int index) {
      this.reader = reader;
      this.blockingQueue = blockingQueue;
      this.encoder = encoder;
      this.dataSet = dataSet;
      this.index = index;
    }

    @Override
    public void run() {
      PublicBAOS timeBAOS = new PublicBAOS();
      PublicBAOS valueBAOS = new PublicBAOS();
      try {
        synchronized (reader) {
          // if the task is submitted, there must be free space in the queue
          // so here we don't need to check whether the queue has free space
          // the reader has next batch
          if ((dataSet.cachedBatchData[index] != null && dataSet.cachedBatchData[index].hasCurrent())
                  || reader.hasNextBatch()) {
            BatchData batchData;
            if (dataSet.cachedBatchData[index] != null && dataSet.cachedBatchData[index].hasCurrent())
              batchData = dataSet.cachedBatchData[index];
            else
              batchData = reader.nextBatch();
            
            int rowCount = 0;
            while (rowCount < dataSet.fetchSize) {
              
              if ((dataSet.limitArray[index] > 0 && dataSet.alreadyReturnedRowNumArray[index] >= dataSet.limitArray[index])) {
                break;
              }
              
              if (batchData != null && batchData.hasCurrent()) {
                if (dataSet.offsetArray[index] == 0) {
                  long time = batchData.currentTime();
                  ReadWriteIOUtils.write(time, timeBAOS);
                  TSDataType type = batchData.getDataType();
                  switch (type) {
                    case INT32:
                      int intValue = batchData.getInt();
                      if (encoder != null && encoder.needEncode(time)) {
                        intValue = encoder.encodeInt(intValue, time);
                      }
                      ReadWriteIOUtils.write(intValue, valueBAOS);
                      break;
                    case INT64:
                      long longValue = batchData.getLong();
                      if (encoder != null && encoder.needEncode(time)) {
                        longValue = encoder.encodeLong(longValue, time);
                      }
                      ReadWriteIOUtils.write(longValue, valueBAOS);
                      break;
                    case FLOAT:
                      float floatValue = batchData.getFloat();
                      if (encoder != null && encoder.needEncode(time)) {
                        floatValue = encoder.encodeFloat(floatValue, time);
                      }
                      ReadWriteIOUtils.write(floatValue, valueBAOS);
                      break;
                    case DOUBLE:
                      double doubleValue = batchData.getDouble();
                      if (encoder != null && encoder.needEncode(time)) {
                        doubleValue = encoder.encodeDouble(doubleValue, time);
                      }
                      ReadWriteIOUtils.write(doubleValue, valueBAOS);
                      break;
                    case BOOLEAN:
                      ReadWriteIOUtils.write(batchData.getBoolean(),
                              valueBAOS);
                      break;
                    case TEXT:
                      ReadWriteIOUtils
                              .write(batchData.getBinary(),
                                      valueBAOS);
                      break;
                    default:
                      throw new UnSupportedDataTypeException(
                              String.format("Data type %s is not supported.", type));
                  }
                }
                batchData.next();
              }
              else {
                batchData = reader.nextBatch();
                dataSet.cachedBatchData[index] = batchData;
                continue;
              }
              if (dataSet.offsetArray[index] == 0) {
                rowCount++;
                if (dataSet.limitArray[index] > 0) {
                  dataSet.alreadyReturnedRowNumArray[index]++;
                }
              } else {
                dataSet.offsetArray[index]--;
              }
            }
            if (rowCount == 0) {
              blockingQueue.put(new Pair(null, null));
              // set the hasRemaining field in reader to false
              // tell the Consumer not to submit another task for this reader any more
              reader.setHasRemaining(false);
              // remove itself from the QueryTaskPoolManager
              reader.setManagedByQueryManager(false);
              return;
            }

            ByteBuffer timeBuffer = ByteBuffer.allocate(timeBAOS.size());
            timeBuffer.put(timeBAOS.getBuf(), 0, timeBAOS.size());
            timeBuffer.flip();
            ByteBuffer valueBuffer = ByteBuffer.allocate(valueBAOS.size());
            valueBuffer.put(valueBAOS.getBuf(), 0, valueBAOS.size());
            valueBuffer.flip();

            Pair<ByteBuffer, ByteBuffer> timeValueBAOSPair = new Pair(timeBuffer, valueBuffer);
            
            blockingQueue.put(timeValueBAOSPair);
            // if the queue also has free space, just submit another itself
            if (blockingQueue.remainingCapacity() > 0) {
              pool.submit(this);
            }
            // the queue has no more space
            // remove itself from the QueryTaskPoolManager
            else {
              reader.setManagedByQueryManager(false);
            }
            return;
          }
          blockingQueue.put(new Pair(null, null));
          // set the hasRemaining field in reader to false
          // tell the Consumer not to submit another task for this reader any more
          reader.setHasRemaining(false);
          // remove itself from the QueryTaskPoolManager
          reader.setManagedByQueryManager(false);
        }
      } catch (InterruptedException e) {
        LOGGER.error("Interrupted while putting into the blocking queue: ", e);
      } catch (IOException e) {
        LOGGER.error("Something gets wrong while reading from the series reader: ", e);
      } catch (Exception e) {
        LOGGER.error("Something gets wrong: ", e);
      }
      
    }
    
  }

  
  private List<ManagedSeriesReader> seriesReaderWithoutValueFilterList;
  
  // Blocking queue list for each time value buffer pair
  private BlockingQueue<Pair<ByteBuffer, ByteBuffer>>[] blockingQueueArray;
  
  private boolean initialized = false;

  private int[] offsetArray;

  private int[] limitArray;

  private int[] alreadyReturnedRowNumArray;

  private BatchData[] cachedBatchData;

  // indicate that there is no more batch data in the corresponding queue
  // in case that the consumer thread is blocked on the queue and won't get runnable any more
  // this field is not same as the `hasRemaining` in SeriesReaderWithoutValueFilter
  // even though the `hasRemaining` in SeriesReaderWithoutValueFilter is false
  // noMoreDataInQueue can still be true
  // its usage is to tell the consumer thread not to call the take() method.
  private boolean[] noMoreDataInQueueArray;

  private int fetchSize;

  // indicate that there is no more batch data in the corresponding queue
  // in case that the consumer thread is blocked on the queue and won't get runnable any more
  // this field is not same as the `hasRemaining` in SeriesReaderWithoutValueFilter
  // even though the `hasRemaining` in SeriesReaderWithoutValueFilter is false
  // noMoreDataInQueue can still be true
  // its usage is to tell the consumer thread not to call the take() method.

  // capacity for blocking queue
  private static final int BLOCKING_QUEUE_CAPACITY = 5;

  private static final QueryTaskPoolManager pool = QueryTaskPoolManager.getInstance();

  private static final Logger LOGGER = LoggerFactory.getLogger(NonAlignEngineDataSet.class);
  
  /**
   * constructor of EngineDataSet.
   *
   * @param paths paths in List structure
   * @param dataTypes time series data type
   * @param readers readers in List(IPointReader) structure
   */
  public NonAlignEngineDataSet(List<Path> paths, List<TSDataType> dataTypes,
      List<ManagedSeriesReader> readers) {
    super(paths, dataTypes);
    this.seriesReaderWithoutValueFilterList = readers;
    blockingQueueArray = new BlockingQueue[readers.size()];
    noMoreDataInQueueArray = new boolean[readers.size()];
    initLimit(super.rowOffset, super.rowLimit, readers.size());

    for (int i = 0; i < seriesReaderWithoutValueFilterList.size(); i++) {
      blockingQueueArray[i] = new LinkedBlockingQueue<>(BLOCKING_QUEUE_CAPACITY);
    }
  }

  private void initLimit(int offset, int limit, int size) {
    offsetArray = new int[size];
    Arrays.fill(offsetArray, offset);
    limitArray = new int[size];
    Arrays.fill(limitArray, limit);
    alreadyReturnedRowNumArray = new int[size];
    cachedBatchData = new BatchData[size];
  }
  
  private void init(WatermarkEncoder encoder, int fetchSize) {
    this.fetchSize = fetchSize;
    for (int i = 0; i < seriesReaderWithoutValueFilterList.size(); i++) {
      ManagedSeriesReader reader = seriesReaderWithoutValueFilterList.get(i);
      reader.setHasRemaining(true);
      reader.setManagedByQueryManager(true);
      pool.submit(new ReadTask(reader, blockingQueueArray[i], encoder, this, i));
    }
    this.initialized = true;
  }
  
  /**
   * for RPC in RawData query between client and server
   * fill time buffers and value buffers
   */
  public TSQueryNonAlignDataSet fillBuffer(int fetchSize, WatermarkEncoder encoder) throws InterruptedException {
    if (!initialized) {
      init(encoder, fetchSize);
    }
    int seriesNum = seriesReaderWithoutValueFilterList.size();
    TSQueryNonAlignDataSet tsQueryNonAlignDataSet = new TSQueryNonAlignDataSet();

    List<ByteBuffer> timeBufferList = new ArrayList<>(seriesNum);
    List<ByteBuffer> valueBufferList = new ArrayList<>(seriesNum);
    
    for (int seriesIndex = 0; seriesIndex < seriesNum; seriesIndex++) {
      if (!noMoreDataInQueueArray[seriesIndex]) {
        Pair<ByteBuffer, ByteBuffer> timeValueByteBufferPair = blockingQueueArray[seriesIndex].take();
        if (timeValueByteBufferPair.left == null || timeValueByteBufferPair.right == null) {
          noMoreDataInQueueArray[seriesIndex] = true;
          timeValueByteBufferPair.left = ByteBuffer.allocate(0);
          timeValueByteBufferPair.right = ByteBuffer.allocate(0);
        }
        timeBufferList.add(timeValueByteBufferPair.left);
        valueBufferList.add(timeValueByteBufferPair.right);
      }
      else {
        timeBufferList.add(ByteBuffer.allocate(0));
        valueBufferList.add(ByteBuffer.allocate(0));
        continue;
      }

      synchronized (seriesReaderWithoutValueFilterList.get(seriesIndex)) {
        if (blockingQueueArray[seriesIndex].remainingCapacity() > 0) {
          ManagedSeriesReader reader = seriesReaderWithoutValueFilterList.get(seriesIndex);
          // if the reader isn't being managed and still has more data,
          // that means this read task leave the pool before because the queue has no more space
          // now we should submit it again
          if (!reader.isManagedByQueryManager() && reader.hasRemaining()) {
            reader.setManagedByQueryManager(true);
            pool.submit(new ReadTask(reader, blockingQueueArray[seriesIndex], 
                encoder, this, seriesIndex));
          }
        }
      }
    }

    // set time buffers, value buffers and bitmap buffers
    tsQueryNonAlignDataSet.setTimeList(timeBufferList);
    tsQueryNonAlignDataSet.setValueList(valueBufferList);

    return tsQueryNonAlignDataSet;
  }


  @Override
  protected boolean hasNextWithoutConstraint() {
    return false;
  }

  @Override
  protected RowRecord nextWithoutConstraint() {
    return null;
  }
  

}
