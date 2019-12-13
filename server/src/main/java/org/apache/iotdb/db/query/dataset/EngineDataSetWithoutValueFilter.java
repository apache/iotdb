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
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;
import org.apache.iotdb.tsfile.read.reader.IBatchReader;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;
import java.util.TreeSet;


public class EngineDataSetWithoutValueFilter extends QueryDataSet {

  private List<IBatchReader> seriesReaderWithoutValueFilterList;

  private TreeSet<Long> timeHeap;

  private BatchData[] cachedBatchDataArray;

  private static final int flag = 0x01;

  /**
   * constructor of EngineDataSetWithoutValueFilter.
   *
   * @param paths paths in List structure
   * @param dataTypes time series data type
   * @param readers readers in List(IPointReader) structure
   * @throws IOException IOException
   */
  public EngineDataSetWithoutValueFilter(List<Path> paths, List<TSDataType> dataTypes,
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
        timeHeapPut(batchData.currentTime());
      }
    }
  }


  /**
   * fill time buffer, value buffers and bitmap buffers
   */
  public TSQueryDataSet fillBuffer(int fetchSize) throws IOException {
    int columnNum = seriesReaderWithoutValueFilterList.size();
    TSQueryDataSet tsQueryDataSet = new TSQueryDataSet();
    // one time column and each value column has a actual value buffer and a bitmap value to indicate whether it is a null
    int columnNumWithTime = columnNum * 2 + 1;
    DataOutputStream[] dataOutputStreams = new DataOutputStream[columnNumWithTime];
    ByteArrayOutputStream[] byteArrayOutputStreams = new ByteArrayOutputStream[columnNumWithTime];
    for (int i = 0; i < columnNumWithTime; i++) {
      byteArrayOutputStreams[i] = new ByteArrayOutputStream();
      dataOutputStreams[i] = new DataOutputStream(byteArrayOutputStreams[i]);
    }

    int rowCount = 0;
    int[] valueOccupation = new int[columnNum];
    // used to record a bitmap for every 8 row record
    int[] bitmap = new int[columnNum];
    for (int i = 0; i < fetchSize; i++) {
      if (!timeHeap.isEmpty()) {
        long minTime = timeHeapGet();
        // use columnOutput to write byte array
        dataOutputStreams[0].writeLong(minTime);
        for (int k = 0; k < cachedBatchDataArray.length; k++) {
          if (cachedBatchDataArray[k] != null && !cachedBatchDataArray[k].hasNext()) {
            if (seriesReaderWithoutValueFilterList.get(k).hasNextBatch())
              cachedBatchDataArray[k] = seriesReaderWithoutValueFilterList.get(k).nextBatch();
            else
              cachedBatchDataArray[k] = null;
          }
          BatchData batchData = cachedBatchDataArray[k];
          DataOutputStream dataOutputStream = dataOutputStreams[2*k + 1]; // DO NOT FORGET +1
          if (batchData == null || batchData.currentTime() != minTime) {
            bitmap[k] =  (bitmap[k] << 1);
            if (batchData != null)
              timeHeapPut(batchData.currentTime());
          } else {
            bitmap[k] =  (bitmap[k] << 1) | flag;
            TSDataType type = batchData.getDataType();
            switch (type) {
              case INT32:
                dataOutputStream.writeInt(batchData.getInt());
                valueOccupation[k] += 4;
                break;
              case INT64:
                dataOutputStream.writeLong(batchData.getLong());
                valueOccupation[k] += 8;
                break;
              case FLOAT:
                dataOutputStream.writeFloat(batchData.getFloat());
                valueOccupation[k] += 4;
                break;
              case DOUBLE:
                dataOutputStream.writeDouble(batchData.getDouble());
                valueOccupation[k] += 8;
                break;
              case BOOLEAN:
                dataOutputStream.writeBoolean(batchData.getBoolean());
                valueOccupation[k] += 1;
                break;
              case TEXT:
                dataOutputStream.writeInt(batchData.getBinary().getLength());
                dataOutputStream.write(batchData.getBinary().getValues());
                valueOccupation[k] = valueOccupation[k] + 4 + batchData.getBinary().getLength();
                break;
              default:
                throw new UnSupportedDataTypeException(
                        String.format("Data type %s is not supported.", type));
            }
          }
        }
        rowCount++;
        if (rowCount % 8 == 0) {
          for (int j = 0; j < bitmap.length; j++) {
            DataOutputStream dataBitmapOutputStream = dataOutputStreams[2*(j+1)];
            dataBitmapOutputStream.writeByte(bitmap[j]);
            // we should clear the bitmap every 8 row record
            bitmap[j] = 0;
          }
        }
      } else {
        break;
      }
    }

    // feed the remaining bitmap
    int remaining = rowCount % 8;
    if (remaining != 0) {
      for (int j = 0; j < bitmap.length; j++) {
        DataOutputStream dataBitmapOutputStream = dataOutputStreams[2*(j+1)];
        dataBitmapOutputStream.writeByte(bitmap[j] << (8-remaining));
      }
    }

    // calculate the time buffer size
    int timeOccupation = rowCount * 8;
    ByteBuffer timeBuffer = ByteBuffer.allocate(timeOccupation);
    timeBuffer.put(byteArrayOutputStreams[0].toByteArray());
    timeBuffer.flip();
    tsQueryDataSet.setTime(timeBuffer);

    // calculate the bitmap buffer size
    int bitmapOccupation = rowCount / 8 + (rowCount % 8 == 0 ? 0 : 1);

    List<ByteBuffer> bitmapList = new LinkedList<>();
    List<ByteBuffer> valueList = new LinkedList<>();
    for (int i = 1; i < byteArrayOutputStreams.length; i += 2) {
      ByteBuffer valueBuffer = ByteBuffer.allocate(valueOccupation[(i-1)/2]);
      valueBuffer.put(byteArrayOutputStreams[i].toByteArray());
      valueBuffer.flip();
      valueList.add(valueBuffer);

      ByteBuffer bitmapBuffer = ByteBuffer.allocate(bitmapOccupation);
      bitmapBuffer.put(byteArrayOutputStreams[i+1].toByteArray());
      bitmapBuffer.flip();
      bitmapList.add(bitmapBuffer);
    }
    tsQueryDataSet.setBitmapList(bitmapList);
    tsQueryDataSet.setValueList(valueList);
    return tsQueryDataSet;
  }

  @Override
  protected boolean hasNextWithoutConstraint() throws IOException {
    throw new IOException("The method can't be invoked, please try to use fillBuffer method directly!");
  }

  @Override
  protected RowRecord nextWithoutConstraint() throws IOException {
    throw new IOException("The method can't be invoked, please try to use fillBuffer method directly!");
  }

  /**
   * keep heap from storing duplicate time.
   */
  private void timeHeapPut(long time) {
    timeHeap.add(time);
  }

  private Long timeHeapGet() {
    return timeHeap.pollFirst();
  }

}
