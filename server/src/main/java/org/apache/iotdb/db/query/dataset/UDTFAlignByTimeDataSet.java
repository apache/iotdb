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
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.qp.physical.crud.UDTFPlan;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.reader.series.IReaderByTimestamp;
import org.apache.iotdb.db.query.reader.series.ManagedSeriesReader;
import org.apache.iotdb.db.query.udf.api.iterator.DataPointIterator;
import org.apache.iotdb.db.tools.watermark.WatermarkEncoder;
import org.apache.iotdb.service.rpc.thrift.TSQueryDataSet;
import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Field;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.read.query.timegenerator.TimeGenerator;
import org.apache.iotdb.tsfile.utils.BytesUtils;
import org.apache.iotdb.tsfile.utils.PublicBAOS;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

public class UDTFAlignByTimeDataSet extends UDTFDataSet implements DirectAlignByTimeDataSet {

  protected TreeSet<Long> timeHeap;

  // execute with value filter
  public UDTFAlignByTimeDataSet(QueryContext context, UDTFPlan udtfPlan,
      List<Path> deduplicatedPaths, List<TSDataType> deduplicatedDataTypes,
      TimeGenerator timestampGenerator, List<IReaderByTimestamp> readersOfSelectedSeries,
      List<Boolean> cached) throws IOException, QueryProcessException {
    super(context, udtfPlan, deduplicatedPaths, deduplicatedDataTypes, timestampGenerator,
        readersOfSelectedSeries, cached);
    initTimeHeap();
  }

  // execute without value filter
  public UDTFAlignByTimeDataSet(QueryContext context, UDTFPlan udtfPlan,
      List<Path> deduplicatedPaths, List<TSDataType> deduplicatedDataTypes,
      List<ManagedSeriesReader> readersOfSelectedSeries) throws QueryProcessException, IOException {
    super(context, udtfPlan, deduplicatedPaths, deduplicatedDataTypes, readersOfSelectedSeries);
    initTimeHeap();
  }

  protected void initTimeHeap() throws IOException {
    timeHeap = new TreeSet<>();
    for (DataPointIterator dataPointIterator : transformedDataColumns) {
      if (dataPointIterator.hasNextPoint()) {
        timeHeap.add(dataPointIterator.nextTime());
      }
    }
  }

  @Override
  public TSQueryDataSet fillBuffer(int fetchSize, WatermarkEncoder encoder) throws IOException {
    TSQueryDataSet tsQueryDataSet = new TSQueryDataSet();

    int columnsNum = transformedDataColumns.length;

    PublicBAOS timeBAOS = new PublicBAOS();
    PublicBAOS[] valueBAOSList = new PublicBAOS[columnsNum];
    PublicBAOS[] bitmapBAOSList = new PublicBAOS[columnsNum];
    for (int i = 0; i < columnsNum; ++i) {
      valueBAOSList[i] = new PublicBAOS();
      bitmapBAOSList[i] = new PublicBAOS();
    }
    int[] currentBitmapList = new int[columnsNum];

    int rowCount = 0;
    while (rowCount < fetchSize
        && (rowLimit <= 0 || alreadyReturnedRowNum < rowLimit)
        && !timeHeap.isEmpty()) {

      long minTime = timeHeap.pollFirst();
      if (rowOffset == 0) {
        timeBAOS.write(BytesUtils.longToBytes(minTime));
      }

      for (int i = 0; i < columnsNum; ++i) {
        DataPointIterator dataPointIterator = transformedDataColumns[i];
        if (!dataPointIterator.hasNextPoint() || dataPointIterator.nextTime() != minTime) {
          if (rowOffset == 0) {
            currentBitmapList[i] = (currentBitmapList[i] << 1);
          }
          continue;
        }
        if (rowOffset == 0) {
          currentBitmapList[i] = (currentBitmapList[i] << 1) | FLAG;
          dataPointIterator.next();
          TSDataType type = transformedDataColumnDataTypes.get(i);
          switch (type) {
            case INT32:
              int intValue = dataPointIterator.currentInt();
              ReadWriteIOUtils.write(encoder != null && encoder.needEncode(minTime)
                  ? encoder.encodeInt(intValue, minTime) : intValue, valueBAOSList[i]);
              break;
            case INT64:
              long longValue = dataPointIterator.currentLong();
              ReadWriteIOUtils.write(encoder != null && encoder.needEncode(minTime)
                  ? encoder.encodeLong(longValue, minTime) : longValue, valueBAOSList[i]);
              break;
            case FLOAT:
              float floatValue = dataPointIterator.currentFloat();
              ReadWriteIOUtils.write(encoder != null && encoder.needEncode(minTime)
                  ? encoder.encodeFloat(floatValue, minTime) : floatValue, valueBAOSList[i]);
              break;
            case DOUBLE:
              double doubleValue = dataPointIterator.currentDouble();
              ReadWriteIOUtils.write(encoder != null && encoder.needEncode(minTime)
                  ? encoder.encodeDouble(doubleValue, minTime) : doubleValue, valueBAOSList[i]);
              break;
            case BOOLEAN:
              ReadWriteIOUtils.write(dataPointIterator.currentBoolean(), valueBAOSList[i]);
              break;
            case TEXT:
              ReadWriteIOUtils.write(dataPointIterator.currentBinary(), valueBAOSList[i]);
              break;
            default:
              throw new UnSupportedDataTypeException(
                  String.format("Data type %s is not supported.", type));
          }
        }

        if (dataPointIterator.hasNextPoint()) {
          timeHeap.add(dataPointIterator.nextTime());
        }
      }

      if (rowOffset == 0) {
        ++rowCount;
        if (rowCount % 8 == 0) {
          for (int i = 0; i < columnsNum; ++i) {
            ReadWriteIOUtils.write((byte) currentBitmapList[i], bitmapBAOSList[i]);
            currentBitmapList[i] = 0;
          }
        }
        if (rowLimit > 0) {
          ++alreadyReturnedRowNum;
        }
      } else {
        --rowOffset;
      }
    }

    /*
     * feed the bitmap with remaining 0 in the right
     * if current bitmap is 00011111 and remaining is 3, after feeding the bitmap is 11111000
     */
    if (rowCount > 0) {
      int remaining = rowCount % 8;
      if (remaining != 0) {
        for (int i = 0; i < columnsNum; ++i) {
          ReadWriteIOUtils
              .write((byte) (currentBitmapList[i] << (8 - remaining)), bitmapBAOSList[i]);
        }
      }
    }

    ByteBuffer timeBuffer = ByteBuffer.allocate(timeBAOS.size());
    timeBuffer.put(timeBAOS.getBuf(), 0, timeBAOS.size());
    timeBuffer.flip();
    tsQueryDataSet.setTime(timeBuffer);

    List<ByteBuffer> valueBufferList = new ArrayList<>();
    List<ByteBuffer> bitmapBufferList = new ArrayList<>();
    for (int i = 0; i < columnsNum; ++i) {
      putPBOSToBuffer(valueBAOSList, valueBufferList, i);
      putPBOSToBuffer(bitmapBAOSList, bitmapBufferList, i);
    }
    tsQueryDataSet.setValueList(valueBufferList);
    tsQueryDataSet.setBitmapList(bitmapBufferList);

    return tsQueryDataSet;
  }

  protected void putPBOSToBuffer(PublicBAOS[] bitmapBAOSList, List<ByteBuffer> bitmapBufferList,
      int tsIndex) {
    ByteBuffer bitmapBuffer = ByteBuffer.allocate(bitmapBAOSList[tsIndex].size());
    bitmapBuffer.put(bitmapBAOSList[tsIndex].getBuf(), 0, bitmapBAOSList[tsIndex].size());
    bitmapBuffer.flip();
    bitmapBufferList.add(bitmapBuffer);
  }

  @Override
  protected boolean hasNextWithoutConstraint() {
    return !timeHeap.isEmpty();
  }

  @Override
  protected RowRecord nextWithoutConstraint() throws IOException {
    long minTime = timeHeap.pollFirst();
    RowRecord rowRecord = new RowRecord(minTime);

    int size = transformedDataColumns.length;
    for (int i = 0; i < size; ++i) {
      DataPointIterator dataPointIterator = transformedDataColumns[i];
      if (!dataPointIterator.hasNextPoint() || dataPointIterator.nextTime() != minTime) {
        rowRecord.addField(new Field(null));
        continue;
      }
      dataPointIterator.next();
      Object value;
      switch (transformedDataColumnDataTypes.get(i)) {
        case INT32:
          value = dataPointIterator.currentInt();
          break;
        case INT64:
          value = dataPointIterator.currentLong();
          break;
        case FLOAT:
          value = dataPointIterator.currentFloat();
          break;
        case DOUBLE:
          value = dataPointIterator.currentDouble();
          break;
        case BOOLEAN:
          value = dataPointIterator.currentBoolean();
          break;
        case TEXT:
          value = dataPointIterator.currentBinary();
          break;
        default:
          throw new UnSupportedDataTypeException("Unsupported data type.");
      }
      rowRecord.addField(value, transformedDataColumnDataTypes.get(i));

      if (dataPointIterator.hasNextPoint()) {
        timeHeap.add(dataPointIterator.nextTime());
      }
    }
    return rowRecord;
  }
}
