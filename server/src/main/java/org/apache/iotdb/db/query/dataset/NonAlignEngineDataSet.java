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
import org.apache.iotdb.db.query.control.QueryTimeManager;
import org.apache.iotdb.db.query.pool.QueryTaskPoolManager;
import org.apache.iotdb.db.query.reader.series.ManagedSeriesReader;
import org.apache.iotdb.db.tools.watermark.WatermarkEncoder;
import org.apache.iotdb.service.rpc.thrift.TSQueryNonAlignDataSet;
import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.BatchData;
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
import java.util.concurrent.atomic.AtomicIntegerArray;

public class NonAlignEngineDataSet extends QueryDataSet implements DirectNonAlignDataSet {

  private class ReadTask extends WrappedRunnable {

    private final ManagedSeriesReader reader;
    private BlockingQueue<Pair<ByteBuffer, ByteBuffer>> blockingQueue;
    private WatermarkEncoder encoder;
    private int index;

    public ReadTask(
        ManagedSeriesReader reader,
        BlockingQueue<Pair<ByteBuffer, ByteBuffer>> blockingQueue,
        WatermarkEncoder encoder,
        int index) {
      this.reader = reader;
      this.blockingQueue = blockingQueue;
      this.encoder = encoder;
      this.index = index;
    }

    @SuppressWarnings("squid:S3776") // Suppress high Cognitive Complexity warning
    @Override
    public void runMayThrow() {
      PublicBAOS timeBAOS = new PublicBAOS();
      PublicBAOS valueBAOS = new PublicBAOS();
      try {
        if (interrupted) {
          // nothing is put here since before reading it the main thread will return
          return;
        }
        synchronized (reader) {
          // if the task is submitted, there must be free space in the queue
          // so here we don't need to check whether the queue has free space
          // the reader has next batch
          if ((cachedBatchData[index] != null && cachedBatchData[index].hasCurrent())
              || reader.hasNextBatch()) {
            BatchData batchData;
            if (cachedBatchData[index] != null && cachedBatchData[index].hasCurrent()) {
              batchData = cachedBatchData[index];
            } else {
              batchData = reader.nextBatch();
            }
            int rowCount = 0;
            while (rowCount < fetchSize) {

              if ((limit > 0 && alreadyReturnedRowNumArray.get(index) >= limit)) {
                break;
              }

              if (batchData != null && batchData.hasCurrent()) {
                if (offsetArray.get(index) == 0) {
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
                      ReadWriteIOUtils.write(batchData.getBoolean(), valueBAOS);
                      break;
                    case TEXT:
                      ReadWriteIOUtils.write(batchData.getBinary(), valueBAOS);
                      break;
                    default:
                      throw new UnSupportedDataTypeException(
                          String.format("Data type %s is not supported.", type));
                  }
                }
                batchData.next();
              } else {
                if (reader.hasNextBatch()) {
                  batchData = reader.nextBatch();
                  cachedBatchData[index] = batchData;
                  continue;
                } else {
                  break;
                }
              }
              if (offsetArray.get(index) == 0) {
                rowCount++;
                if (limit > 0) {
                  alreadyReturnedRowNumArray.incrementAndGet(index);
                }
              } else {
                offsetArray.decrementAndGet(index);
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

            ByteBuffer timeBuffer = ByteBuffer.wrap(timeBAOS.getBuf());
            timeBuffer.limit(timeBAOS.size());
            ByteBuffer valueBuffer = ByteBuffer.wrap(valueBAOS.getBuf());
            valueBuffer.limit(valueBAOS.size());

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
        Thread.currentThread().interrupt();
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

  private AtomicIntegerArray offsetArray;

  private int limit;

  private AtomicIntegerArray alreadyReturnedRowNumArray;

  private BatchData[] cachedBatchData;

  // indicate that there is no more batch data in the corresponding queue
  // in case that the consumer thread is blocked on the queue and won't get runnable any more
  // this field is not same as the `hasRemaining` in SeriesReaderWithoutValueFilter
  // even though the `hasRemaining` in SeriesReaderWithoutValueFilter is false
  // noMoreDataInQueue can still be true
  // its usage is to tell the consumer thread not to call the take() method.
  private boolean[] noMoreDataInQueueArray;

  private int fetchSize;

  // capacity for blocking queue
  private static final int BLOCKING_QUEUE_CAPACITY = 5;

  private final long queryId;
  /** flag that main thread is interrupted or not */
  private volatile boolean interrupted = false;

  private static final QueryTaskPoolManager pool = QueryTaskPoolManager.getInstance();

  private static final Logger LOGGER = LoggerFactory.getLogger(NonAlignEngineDataSet.class);

  /**
   * constructor of EngineDataSet.
   *
   * @param paths paths in List structure
   * @param dataTypes time series data type
   * @param readers readers in List(IPointReader) structure
   */
  public NonAlignEngineDataSet(
      long queryId,
      List<PartialPath> paths,
      List<TSDataType> dataTypes,
      List<ManagedSeriesReader> readers) {
    super(new ArrayList<>(paths), dataTypes);
    this.queryId = queryId;
    this.seriesReaderWithoutValueFilterList = readers;
    blockingQueueArray = new BlockingQueue[readers.size()];
    noMoreDataInQueueArray = new boolean[readers.size()];
    for (int i = 0; i < seriesReaderWithoutValueFilterList.size(); i++) {
      blockingQueueArray[i] = new LinkedBlockingQueue<>(BLOCKING_QUEUE_CAPACITY);
    }
  }

  private void initLimit(int offset, int limit, int size) {
    int[] offsetArrayTemp = new int[size];
    Arrays.fill(offsetArrayTemp, offset);
    offsetArray = new AtomicIntegerArray(offsetArrayTemp);
    this.limit = limit;
    this.alreadyReturnedRowNumArray = new AtomicIntegerArray(size);
    cachedBatchData = new BatchData[size];
  }

  private void init(WatermarkEncoder encoder, int fetchSize) {
    QueryTimeManager.checkQueryAlive(queryId);
    initLimit(super.rowOffset, super.rowLimit, seriesReaderWithoutValueFilterList.size());
    this.fetchSize = fetchSize;
    for (int i = 0; i < seriesReaderWithoutValueFilterList.size(); i++) {
      ManagedSeriesReader reader = seriesReaderWithoutValueFilterList.get(i);
      reader.setHasRemaining(true);
      reader.setManagedByQueryManager(true);
      pool.submit(new ReadTask(reader, blockingQueueArray[i], encoder, i));
    }
    this.initialized = true;
  }

  /** for RPC in RawData query between client and server fill time buffers and value buffers */
  @Override
  public TSQueryNonAlignDataSet fillBuffer(int fetchSize, WatermarkEncoder encoder)
      throws InterruptedException {
    if (!initialized) {
      init(encoder, fetchSize);
    }
    int seriesNum = seriesReaderWithoutValueFilterList.size();
    TSQueryNonAlignDataSet tsQueryNonAlignDataSet = new TSQueryNonAlignDataSet();

    List<ByteBuffer> timeBufferList = new ArrayList<>(seriesNum);
    List<ByteBuffer> valueBufferList = new ArrayList<>(seriesNum);

    for (int seriesIndex = 0; seriesIndex < seriesNum; seriesIndex++) {
      if (!noMoreDataInQueueArray[seriesIndex]) {
        // check the interrupted status of query before take next batch
        QueryTimeManager.checkQueryAlive(queryId);
        Pair<ByteBuffer, ByteBuffer> timeValueByteBufferPair =
            blockingQueueArray[seriesIndex].take();
        if (timeValueByteBufferPair.left == null || timeValueByteBufferPair.right == null) {
          noMoreDataInQueueArray[seriesIndex] = true;
          timeValueByteBufferPair.left = ByteBuffer.allocate(0);
          timeValueByteBufferPair.right = ByteBuffer.allocate(0);
        }
        timeBufferList.add(timeValueByteBufferPair.left);
        valueBufferList.add(timeValueByteBufferPair.right);
      } else {
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
            pool.submit(
                new ReadTask(reader, blockingQueueArray[seriesIndex], encoder, seriesIndex));
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
  public boolean hasNextWithoutConstraint() {
    return false;
  }

  @Override
  public RowRecord nextWithoutConstraint() {
    return null;
  }
}
