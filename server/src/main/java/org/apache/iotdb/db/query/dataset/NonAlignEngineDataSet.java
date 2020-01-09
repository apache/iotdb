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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

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

public class NonAlignEngineDataSet extends QueryDataSet {
  
  private static class ReadTask implements Runnable {
    
    private final ManagedSeriesReader reader;
    private BlockingQueue<Pair<PublicBAOS, PublicBAOS>> blockingQueue;
    private WatermarkEncoder encoder;
    
    public ReadTask(ManagedSeriesReader reader, 
        BlockingQueue<Pair<PublicBAOS, PublicBAOS>> blockingQueue,
        WatermarkEncoder encoder) {
      this.reader = reader;
      this.blockingQueue = blockingQueue;
      this.encoder = encoder;
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
          while (reader.hasNextBatch()) {
            BatchData batchData = reader.nextBatch();
            // iterate until we get first batch data with valid value
            if (batchData.isEmpty()) {
              continue;
            }
            while (batchData != null && batchData.hasCurrent()) {
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
              batchData.next();
            }
            Pair<PublicBAOS, PublicBAOS> timeValueBAOSPair = new Pair(timeBAOS, valueBAOS);
            
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
          blockingQueue.put(new Pair(timeBAOS, valueBAOS));
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
  private BlockingQueue<Pair<PublicBAOS, PublicBAOS>>[] blockingQueueArray;
  
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
      List<ManagedSeriesReader> readers) throws InterruptedException {
    super(paths, dataTypes);
    this.seriesReaderWithoutValueFilterList = readers;
    blockingQueueArray = new BlockingQueue[readers.size()];
    for (int i = 0; i < seriesReaderWithoutValueFilterList.size(); i++) {
      blockingQueueArray[i] = new LinkedBlockingQueue<>(BLOCKING_QUEUE_CAPACITY);
    }
    init();
  }
  
  private void init() throws InterruptedException {
    for (int i = 0; i < seriesReaderWithoutValueFilterList.size(); i++) {
      ManagedSeriesReader reader = seriesReaderWithoutValueFilterList.get(i);
      reader.setHasRemaining(true);
      reader.setManagedByQueryManager(true);
      pool.submit(new ReadTask(reader, blockingQueueArray[i], null));
    }
    
  }
  
  /**
   * for RPC in RawData query between client and server
   * fill time buffers and value buffers
   */
  public TSQueryNonAlignDataSet fillBuffer(int fetchSize, WatermarkEncoder encoder) throws IOException, InterruptedException {
    int seriesNum = seriesReaderWithoutValueFilterList.size();
    TSQueryNonAlignDataSet tsQueryNonAlignDataSet = new TSQueryNonAlignDataSet();

    PublicBAOS[] timeBAOSList = new PublicBAOS[seriesNum];
    PublicBAOS[] valueBAOSList = new PublicBAOS[seriesNum];
    int rowCount = 0;
    while (rowCount < fetchSize) {

      if ((rowLimit > 0 && alreadyReturnedRowNum >= rowLimit) || rowCount >= 1) {
        break;
      }

      for (int seriesIndex = 0; seriesIndex < seriesNum; seriesIndex++) {
        synchronized (seriesReaderWithoutValueFilterList.get(seriesIndex)) {
          if (blockingQueueArray[seriesIndex].remainingCapacity() > 0) {
            ManagedSeriesReader reader = seriesReaderWithoutValueFilterList.get(seriesIndex);
            // if the reader isn't being managed and still has more data,
            // that means this read task leave the pool before because the queue has no more space
            // now we should submit it again
            reader.setHasRemaining(true);
            if (!reader.isManagedByQueryManager() && reader.hasRemaining()) {
              reader.setManagedByQueryManager(true);
              pool.submit(new ReadTask(reader, blockingQueueArray[seriesIndex], encoder));
            }
          }
        }
        Pair<PublicBAOS, PublicBAOS> timevalueBAOSPair = blockingQueueArray[seriesIndex].take();
        timeBAOSList[seriesIndex] = timevalueBAOSPair.left;
        valueBAOSList[seriesIndex] = timevalueBAOSPair.right;
      }
      rowCount++;
      if (rowLimit > 0) {
        alreadyReturnedRowNum++;
      }

    }

    List<ByteBuffer> timeBufferList = new ArrayList<>();
    List<ByteBuffer> valueBufferList = new ArrayList<>();

    for (int seriesIndex = 0; seriesIndex < seriesNum; seriesIndex++) {
      // add time buffer of current series
      putPBOSToBuffer(timeBAOSList, timeBufferList, seriesIndex);

      // add value buffer of current series
      putPBOSToBuffer(valueBAOSList, valueBufferList, seriesIndex);
    }

    // set time buffers, value buffers and bitmap buffers
    tsQueryNonAlignDataSet.setTimeList(timeBufferList);
    tsQueryNonAlignDataSet.setValueList(valueBufferList);

    return tsQueryNonAlignDataSet;
  }
  
  private void putPBOSToBuffer(PublicBAOS[] aBAOSList, List<ByteBuffer> aBufferList,
      int tsIndex) {
    ByteBuffer aBuffer = ByteBuffer.allocate(aBAOSList[tsIndex].size());
    aBuffer.put(aBAOSList[tsIndex].getBuf(), 0, aBAOSList[tsIndex].size());
    aBuffer.flip();
    aBufferList.add(aBuffer);
  }

  @Override
  protected boolean hasNextWithoutConstraint() throws IOException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  protected RowRecord nextWithoutConstraint() throws IOException {
    // TODO Auto-generated method stub
    return null;
  }
  

}
