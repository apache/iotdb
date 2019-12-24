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
import java.util.TreeSet;
import org.apache.iotdb.db.tools.watermark.WatermarkEncoder;
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
        // TODO here bug batchData may be null because of hasNextBatch of seqReader
      }
    }
  }


  /**
   * for RPC in RawData query between client and server
   * fill time buffer, value buffers and bitmap buffers
   */
  public TSQueryDataSet fillBuffer(int fetchSize, WatermarkEncoder encoder) throws IOException {
    int seriesNum = seriesReaderWithoutValueFilterList.size();
    TSQueryDataSet tsQueryDataSet = new TSQueryDataSet();

    PublicBAOS timeBAOS = new PublicBAOS();
    PublicBAOS[] valueBAOSList = new PublicBAOS[seriesNum];
    PublicBAOS[] bitmapBAOSList = new PublicBAOS[seriesNum];

    for (int seriesIndex = 0; seriesIndex < seriesNum; seriesIndex++) {
      valueBAOSList[seriesIndex] = new PublicBAOS();
      bitmapBAOSList[seriesIndex] = new PublicBAOS();
    }

    // used to record a bitmap for every 8 row record
    int[] currentBitmapList = new int[seriesNum];
    int rowCount = 0;
    while (rowCount < fetchSize) {

      if ((rowLimit > 0 && alreadyReturnedRowNum >= rowLimit) || timeHeap.isEmpty()) {
        break;
      }

      long minTime = timeHeap.pollFirst();

      if (rowOffset == 0) {
        timeBAOS.write(BytesUtils.longToBytes(minTime));
      }

      for (int seriesIndex = 0; seriesIndex < seriesNum; seriesIndex++) {
        if (cachedBatchDataArray[seriesIndex] == null
            || !cachedBatchDataArray[seriesIndex].hasCurrent()
            || cachedBatchDataArray[seriesIndex].currentTime() != minTime) {
          // current batch is empty or does not have value at minTime
          if (rowOffset == 0) {
            currentBitmapList[seriesIndex] = (currentBitmapList[seriesIndex] << 1);
          }
        } else {
          // current batch has value at minTime, consume current value
          if (rowOffset == 0) {
            currentBitmapList[seriesIndex] = (currentBitmapList[seriesIndex] << 1) | FLAG;
            TSDataType type = cachedBatchDataArray[seriesIndex].getDataType();
            switch (type) {
              case INT32:
                int intValue = cachedBatchDataArray[seriesIndex].getInt();
                if (encoder != null) {
                  intValue = encoder.encodeInt(intValue, minTime);
                }
                ReadWriteIOUtils.write(intValue, valueBAOSList[seriesIndex]);
                break;
              case INT64:
                long longValue = cachedBatchDataArray[seriesIndex].getLong();
                if (encoder != null) {
                  longValue = encoder.encodeLong(longValue, minTime);
                }
                ReadWriteIOUtils.write(longValue, valueBAOSList[seriesIndex]);
                break;
              case FLOAT:
                float floatValue = cachedBatchDataArray[seriesIndex].getFloat();
                if (encoder != null) {
                  floatValue = encoder.encodeFloat(floatValue, minTime);
                }
                ReadWriteIOUtils.write(floatValue, valueBAOSList[seriesIndex]);
                break;
              case DOUBLE:
                double doubleValue = cachedBatchDataArray[seriesIndex].getDouble();
                if (encoder != null) {
                  doubleValue = encoder.encodeDouble(doubleValue, minTime);
                }
                ReadWriteIOUtils.write(doubleValue, valueBAOSList[seriesIndex]);
                break;
              case BOOLEAN:
                ReadWriteIOUtils.write(cachedBatchDataArray[seriesIndex].getBoolean(),
                    valueBAOSList[seriesIndex]);
                break;
              case TEXT:
                ReadWriteIOUtils
                    .write(cachedBatchDataArray[seriesIndex].getBinary(),
                        valueBAOSList[seriesIndex]);
                break;
              default:
                throw new UnSupportedDataTypeException(
                    String.format("Data type %s is not supported.", type));
            }
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

      if (rowOffset == 0) {
        rowCount++;
        if (rowCount % 8 == 0) {
          for (int seriesIndex = 0; seriesIndex < seriesNum; seriesIndex++) {
            ReadWriteIOUtils
                .write((byte) currentBitmapList[seriesIndex], bitmapBAOSList[seriesIndex]);
            // we should clear the bitmap every 8 row record
            currentBitmapList[seriesIndex] = 0;
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
        for (int seriesIndex = 0; seriesIndex < seriesNum; seriesIndex++) {
          ReadWriteIOUtils.write((byte) (currentBitmapList[seriesIndex] << (8 - remaining)),
              bitmapBAOSList[seriesIndex]);
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

    for (
        int tsIndex = 0;
        tsIndex < seriesNum; tsIndex++) {

      // add value buffer of current series
      putPBOSToBuffer(valueBAOSList, valueBufferList, tsIndex);

      // add bitmap buffer of current series
      putPBOSToBuffer(bitmapBAOSList, bitmapBufferList, tsIndex);
    }

    // set value buffers and bitmap buffers
    tsQueryDataSet.setValueList(valueBufferList);
    tsQueryDataSet.setBitmapList(bitmapBufferList);

    return tsQueryDataSet;
  }

  private void putPBOSToBuffer(PublicBAOS[] bitmapBAOSList, List<ByteBuffer> bitmapBufferList,
      int tsIndex) {
    ByteBuffer bitmapBuffer = ByteBuffer.allocate(bitmapBAOSList[tsIndex].size());
    bitmapBuffer.put(bitmapBAOSList[tsIndex].getBuf(), 0, bitmapBAOSList[tsIndex].size());
    bitmapBuffer.flip();
    bitmapBufferList.add(bitmapBuffer);
  }


  /**
   * for spark/hadoop/hive integration and test
   */
  @Override
  protected boolean hasNextWithoutConstraint() {
    return !timeHeap.isEmpty();
  }

  /**
   * for spark/hadoop/hive integration and test
   */
  @Override
  protected RowRecord nextWithoutConstraint() throws IOException {
    int seriesNum = seriesReaderWithoutValueFilterList.size();

    long minTime = timeHeap.pollFirst();

    RowRecord record = new RowRecord(minTime);

    for (int seriesIndex = 0; seriesIndex < seriesNum; seriesIndex++) {
      if (cachedBatchDataArray[seriesIndex] == null
          || !cachedBatchDataArray[seriesIndex].hasCurrent()
          || cachedBatchDataArray[seriesIndex].currentTime() != minTime) {
        record.addField(new Field(null));
      } else {

        record.addField(
            getField(cachedBatchDataArray[seriesIndex].currentValue(), dataTypes.get(seriesIndex)));

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
