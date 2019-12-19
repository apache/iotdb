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

import org.apache.iotdb.service.rpc.thrift.TSQueryDataSet;
import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.common.Field;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;
import org.apache.iotdb.tsfile.read.reader.IBatchReader;
import org.apache.iotdb.tsfile.utils.BytesUtils;
import org.apache.iotdb.tsfile.utils.PublicBAOS;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeSet;


public class NewEngineDataSetWithoutValueFilter extends QueryDataSet {

  private List<IBatchReader> seriesReaderWithoutValueFilterList;

  private TreeSet<Long> timeHeap;

  private BatchData[] cachedBatchDataArray;

  private static final int FLAG = 0x01;

  /**
   * constructor of EngineDataSetWithoutValueFilter.
   *
   * @param paths paths in List structure
   * @param dataTypes time series data type
   * @param readers readers in List(IPointReader) structure
   * @throws IOException IOException
   */
  public NewEngineDataSetWithoutValueFilter(List<Path> paths, List<TSDataType> dataTypes,
      List<IBatchReader> readers)
      throws IOException {
    super(paths, dataTypes);
    this.seriesReaderWithoutValueFilterList = readers;
    initHeap();
  }

  private void initHeap() throws IOException {
    timeHeap = new TreeSet<>();
    cachedBatchDataArray = new BatchData[seriesReaderWithoutValueFilterList.size()];

    for (int i = 0; i < seriesReaderWithoutValueFilterList.size(); i++) {
      IBatchReader reader = seriesReaderWithoutValueFilterList.get(i);
      if (reader.hasNextBatch()) {
        BatchData batchData = reader.nextBatch();
        cachedBatchDataArray[i] = batchData;
        timeHeap.add(batchData.currentTime());
      }
    }
  }


  /**
   * for RPC in RawData query between client and server
   * fill time buffer, value buffers and bitmap buffers
   */
  public TSQueryDataSet fillBuffer(int fetchSize) throws IOException {
    int seriesNum = seriesReaderWithoutValueFilterList.size();
    TSQueryDataSet tsQueryDataSet = new TSQueryDataSet();

    PublicBAOS timeBAOS = new PublicBAOS();
    PublicBAOS[] valueBAOSList = new PublicBAOS[seriesNum];
    PublicBAOS[] bitmapBAOSList = new PublicBAOS[seriesNum];

    for (int seriesIndex = 0; seriesIndex < seriesNum; seriesIndex++) {
      valueBAOSList[seriesIndex] = new PublicBAOS();
      bitmapBAOSList[seriesIndex] = new PublicBAOS();
    }

    int rowCount = 0;

    // used to record a bitmap for every 8 row record
    int[] currentBitmapList = new int[seriesNum];
    for (int i = 0; i < fetchSize; i++) {

      if (timeHeap.isEmpty()) {
        break;
      }

      long minTime = timeHeap.pollFirst();

      timeBAOS.write(BytesUtils.longToBytes(minTime));

      for (int seriesIndex = 0; seriesIndex < seriesNum; seriesIndex++) {
        if (cachedBatchDataArray[seriesIndex] == null
            || !cachedBatchDataArray[seriesIndex].hasCurrent()
            || cachedBatchDataArray[seriesIndex].currentTime() != minTime) {
          // current batch is empty or does not have value at minTime
          currentBitmapList[seriesIndex] = (currentBitmapList[seriesIndex] << 1);
        } else {
          // current batch has value at minTime, consume current value
          currentBitmapList[seriesIndex] = (currentBitmapList[seriesIndex] << 1) | FLAG;
          TSDataType type = cachedBatchDataArray[seriesIndex].getDataType();
          switch (type) {
            case INT32:
              ReadWriteIOUtils
                  .write(cachedBatchDataArray[seriesIndex].getInt(), valueBAOSList[seriesIndex]);
              break;
            case INT64:
              ReadWriteIOUtils
                  .write(cachedBatchDataArray[seriesIndex].getLong(), valueBAOSList[seriesIndex]);
              break;
            case FLOAT:
              ReadWriteIOUtils
                  .write(cachedBatchDataArray[seriesIndex].getFloat(), valueBAOSList[seriesIndex]);
              break;
            case DOUBLE:
              ReadWriteIOUtils
                  .write(cachedBatchDataArray[seriesIndex].getDouble(), valueBAOSList[seriesIndex]);
              break;
            case BOOLEAN:
              ReadWriteIOUtils.write(cachedBatchDataArray[seriesIndex].getBoolean(),
                  valueBAOSList[seriesIndex]);
              break;
            case TEXT:
              ReadWriteIOUtils
                  .write(cachedBatchDataArray[seriesIndex].getBinary(), valueBAOSList[seriesIndex]);
              break;
            default:
              throw new UnSupportedDataTypeException(
                  String.format("Data type %s is not supported.", type));
          }

          // move next
          cachedBatchDataArray[seriesIndex].next();

          // get next batch if current batch is empty
          if (!cachedBatchDataArray[seriesIndex].hasCurrent()) {
            if (seriesReaderWithoutValueFilterList.get(seriesIndex).hasNextBatch()) {
              cachedBatchDataArray[seriesIndex] = seriesReaderWithoutValueFilterList
                  .get(seriesIndex).nextBatch();
            }
          }

          // try to put the next timestamp into the heap
          if (cachedBatchDataArray[seriesIndex].hasCurrent()) {
            timeHeap.add(cachedBatchDataArray[seriesIndex].currentTime());
          }
        }
      }

      rowCount++;
      if (rowCount % 8 == 0) {
        for (int seriesIndex = 0; seriesIndex < seriesNum; seriesIndex++) {
          ReadWriteIOUtils
              .write((byte) currentBitmapList[seriesIndex], bitmapBAOSList[seriesIndex]);
          // we should clear the bitmap every 8 row record
          currentBitmapList[seriesIndex] = 0;
        }
      }
    }

    /*
     * feed the bitmap with remaining 0 in the right
     * if current bitmap is 00011111 and remaining is 3, after feeding the bitmap is 11111000
     */
    int remaining = rowCount % 8;
    if (remaining != 0) {
      for (int seriesIndex = 0; seriesIndex < seriesNum; seriesIndex++) {
        ReadWriteIOUtils.write((byte) (currentBitmapList[seriesIndex] << (8 - remaining)),
            bitmapBAOSList[seriesIndex]);
      }
    }

    // set time buffer
    ByteBuffer timeBuffer = ByteBuffer.allocate(timeBAOS.size());
    timeBuffer.put(timeBAOS.toByteArray());
    timeBuffer.flip();
    tsQueryDataSet.setTime(timeBuffer);

    List<ByteBuffer> valueBufferList = new ArrayList<>();
    List<ByteBuffer> bitmapBufferList = new ArrayList<>();

    for (int seriesIndex = 0; seriesIndex < seriesNum; seriesIndex++) {

      // add value buffer of current series
      ByteBuffer valueBuffer = ByteBuffer.allocate(valueBAOSList[seriesIndex].size());
      valueBuffer.put(valueBAOSList[seriesIndex].toByteArray());
      valueBuffer.flip();
      valueBufferList.add(valueBuffer);

      // add bitmap buffer of current series
      ByteBuffer bitmapBuffer = ByteBuffer.allocate(bitmapBAOSList[seriesIndex].size());
      bitmapBuffer.put(bitmapBAOSList[seriesIndex].toByteArray());
      bitmapBuffer.flip();
      bitmapBufferList.add(bitmapBuffer);
    }

    // set value buffers and bitmap buffers
    tsQueryDataSet.setValueList(valueBufferList);
    tsQueryDataSet.setBitmapList(bitmapBufferList);

    return tsQueryDataSet;
  }


  /**
   * for test
   */
  @Override protected boolean hasNextWithoutConstraint() {
    return !timeHeap.isEmpty();
  }

  /**
   * for test
   */
  @Override protected RowRecord nextWithoutConstraint() throws IOException {
    int seriesNum = seriesReaderWithoutValueFilterList.size();

    long minTime = timeHeap.pollFirst();

    RowRecord record = new RowRecord(minTime);

    for (int seriesIndex = 0; seriesIndex < seriesNum; seriesIndex++) {
      if (cachedBatchDataArray[seriesIndex] == null
          || !cachedBatchDataArray[seriesIndex].hasCurrent()
          || cachedBatchDataArray[seriesIndex].currentTime() != minTime) {
        record.addField(new Field(null));
      } else {

        record.addField(getField(cachedBatchDataArray[seriesIndex].currentValue(), dataTypes.get(seriesIndex)));

        // move next
        cachedBatchDataArray[seriesIndex].next();

        // get next batch if current batch is empty
        if (!cachedBatchDataArray[seriesIndex].hasCurrent()) {
          if (seriesReaderWithoutValueFilterList.get(seriesIndex).hasNextBatch()) {
            cachedBatchDataArray[seriesIndex] = seriesReaderWithoutValueFilterList
                .get(seriesIndex).nextBatch();
          }
        }

        // try to put the next timestamp into the heap
        if (cachedBatchDataArray[seriesIndex].hasCurrent()) {
          timeHeap.add(cachedBatchDataArray[seriesIndex].currentTime());
        }
      }
    }

    return record;
  }

}
