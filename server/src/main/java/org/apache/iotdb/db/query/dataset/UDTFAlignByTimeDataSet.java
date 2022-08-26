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

import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.mpp.transformation.api.LayerPointReader;
import org.apache.iotdb.db.mpp.transformation.dag.input.IUDFInputDataSet;
import org.apache.iotdb.db.qp.physical.crud.UDTFPlan;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.reader.series.IReaderByTimestamp;
import org.apache.iotdb.db.query.reader.series.ManagedSeriesReader;
import org.apache.iotdb.db.tools.watermark.WatermarkEncoder;
import org.apache.iotdb.db.utils.datastructure.TimeSelector;
import org.apache.iotdb.service.rpc.thrift.TSQueryDataSet;
import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.read.query.timegenerator.TimeGenerator;
import org.apache.iotdb.tsfile.utils.BytesUtils;
import org.apache.iotdb.tsfile.utils.PublicBAOS;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class UDTFAlignByTimeDataSet extends UDTFDataSet implements DirectAlignByTimeDataSet {

  protected TimeSelector timeHeap;
  private final boolean keepNull;

  /** execute with value filter */
  public UDTFAlignByTimeDataSet(
      QueryContext context,
      UDTFPlan udtfPlan,
      TimeGenerator timestampGenerator,
      List<IReaderByTimestamp> readersOfSelectedSeries,
      List<List<Integer>> readerToIndexList,
      List<Boolean> cached)
      throws IOException, QueryProcessException {
    super(
        context,
        udtfPlan,
        udtfPlan.getDeduplicatedPaths(),
        udtfPlan.getDeduplicatedDataTypes(),
        timestampGenerator,
        readersOfSelectedSeries,
        readerToIndexList,
        cached);
    keepNull = false;
    initTimeHeap();
  }

  /** execute without value filter */
  public UDTFAlignByTimeDataSet(
      QueryContext context, UDTFPlan udtfPlan, List<ManagedSeriesReader> readersOfSelectedSeries)
      throws QueryProcessException, IOException, InterruptedException {
    super(
        context,
        udtfPlan,
        udtfPlan.getDeduplicatedPaths(),
        udtfPlan.getDeduplicatedDataTypes(),
        readersOfSelectedSeries);
    keepNull = false;
    initTimeHeap();
  }

  public UDTFAlignByTimeDataSet(
      QueryContext context, UDTFPlan udtfPlan, IUDFInputDataSet inputDataSet, boolean keepNull)
      throws QueryProcessException, IOException {
    super(context, udtfPlan, inputDataSet);
    this.keepNull = keepNull;
    initTimeHeap();
  }

  protected void initTimeHeap() throws IOException, QueryProcessException {
    timeHeap = new TimeSelector(transformers.length << 1, true);
    for (LayerPointReader reader : transformers) {
      iterateReaderToNextValid(reader);
    }
  }

  @Override
  public TSQueryDataSet fillBuffer(int fetchSize, WatermarkEncoder encoder)
      throws IOException, QueryProcessException {
    TSQueryDataSet tsQueryDataSet = new TSQueryDataSet();

    int columnsNum = transformers.length;

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
      if (withoutAllNull || withoutAnyNull) {
        int nullFieldsCnt = 0, index = 0;
        for (LayerPointReader reader : transformers) {
          if (withoutNullColumnsIndex != null && !withoutNullColumnsIndex.contains(index)) {
            index++;
            continue;
          }
          if (!reader.next() || reader.currentTime() != minTime || reader.isCurrentNull()) {
            nullFieldsCnt++;
          }
          index++;
        }
        // In method QueryDataSetUtils.convertQueryDataSetByFetchSize(), we fetch a row and can
        // easily
        // use rowRecord.isAllNull() or rowRecord.hasNullField() to judge whether this row should be
        // kept with clause 'with null'.
        // Here we get a timestamp first and then construct the row column by column.
        // We don't record this row when nullFieldsCnt > 0 and withoutAnyNull == true
        // or (
        //        (
        //            (withoutNullColumnsIndex != null && nullFieldsCnt ==
        // withoutNullColumnsIndex.size())
        //            or
        //            (withoutNullColumnsIndex == null && nullFieldsCnt == columnsNum)
        //        )
        //        and withoutAllNull = true
        //     )
        if ((((withoutNullColumnsIndex != null && nullFieldsCnt == withoutNullColumnsIndex.size())
                    || (withoutNullColumnsIndex == null && nullFieldsCnt == columnsNum))
                && withoutAllNull)
            || (nullFieldsCnt > 0 && withoutAnyNull)) {
          for (LayerPointReader reader : transformers) {
            // if reader.currentTime() == minTime, it means that the value at this timestamp should
            // not be kept, thus we need to prepare next data point.
            if (reader.next() && reader.currentTime() == minTime) {
              reader.readyForNext();
              iterateReaderToNextValid(reader);
            }
          }
          continue;
        }
      }
      if (rowOffset == 0) {
        timeBAOS.write(BytesUtils.longToBytes(minTime));
      }

      for (int i = 0; i < columnsNum; ++i) {
        LayerPointReader reader = transformers[i];

        if (!reader.next() || reader.currentTime() != minTime) {
          if (rowOffset == 0) {
            currentBitmapList[i] = (currentBitmapList[i] << 1);
          }
          continue;
        }

        if (rowOffset == 0) {
          if (!reader.isCurrentNull()) {
            currentBitmapList[i] = (currentBitmapList[i] << 1) | FLAG;
            TSDataType type = reader.getDataType();
            switch (type) {
              case INT32:
                int intValue = reader.currentInt();
                ReadWriteIOUtils.write(
                    encoder != null && encoder.needEncode(minTime)
                        ? encoder.encodeInt(intValue, minTime)
                        : intValue,
                    valueBAOSList[i]);
                break;
              case INT64:
                long longValue = reader.currentLong();
                ReadWriteIOUtils.write(
                    encoder != null && encoder.needEncode(minTime)
                        ? encoder.encodeLong(longValue, minTime)
                        : longValue,
                    valueBAOSList[i]);
                break;
              case FLOAT:
                float floatValue = reader.currentFloat();
                ReadWriteIOUtils.write(
                    encoder != null && encoder.needEncode(minTime)
                        ? encoder.encodeFloat(floatValue, minTime)
                        : floatValue,
                    valueBAOSList[i]);
                break;
              case DOUBLE:
                double doubleValue = reader.currentDouble();
                ReadWriteIOUtils.write(
                    encoder != null && encoder.needEncode(minTime)
                        ? encoder.encodeDouble(doubleValue, minTime)
                        : doubleValue,
                    valueBAOSList[i]);
                break;
              case BOOLEAN:
                ReadWriteIOUtils.write(reader.currentBoolean(), valueBAOSList[i]);
                break;
              case TEXT:
                ReadWriteIOUtils.write(reader.currentBinary(), valueBAOSList[i]);
                break;
              default:
                throw new UnSupportedDataTypeException(
                    String.format("Data type %s is not supported.", type));
            }
          } else {
            // there's no data in the current field, so put a null placeholder 0x00
            currentBitmapList[i] = (currentBitmapList[i] << 1);
          }
        }

        reader.readyForNext();
        iterateReaderToNextValid(reader);
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

      queryDataSetInputLayer.updateRowRecordListEvictionUpperBound();
    }

    /*
     * feed the bitmap with remaining 0 in the right
     * if current bitmap is 00011111 and remaining is 3, after feeding the bitmap is 11111000
     */
    if (rowCount > 0) {
      int remaining = rowCount % 8;
      if (remaining != 0) {
        for (int i = 0; i < columnsNum; ++i) {
          ReadWriteIOUtils.write(
              (byte) (currentBitmapList[i] << (8 - remaining)), bitmapBAOSList[i]);
        }
      }
    }

    return packBuffer(tsQueryDataSet, timeBAOS, valueBAOSList, bitmapBAOSList);
  }

  protected TSQueryDataSet packBuffer(
      TSQueryDataSet tsQueryDataSet,
      PublicBAOS timeBAOS,
      PublicBAOS[] valueBAOSList,
      PublicBAOS[] bitmapBAOSList) {
    int columnsNum = transformers.length;

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

  protected void putPBOSToBuffer(
      PublicBAOS[] bitmapBAOSList, List<ByteBuffer> bitmapBufferList, int tsIndex) {
    ByteBuffer bitmapBuffer = ByteBuffer.allocate(bitmapBAOSList[tsIndex].size());
    bitmapBuffer.put(bitmapBAOSList[tsIndex].getBuf(), 0, bitmapBAOSList[tsIndex].size());
    bitmapBuffer.flip();
    bitmapBufferList.add(bitmapBuffer);
  }

  @Override
  public boolean hasNextWithoutConstraint() {
    return !timeHeap.isEmpty();
  }

  @Override
  public RowRecord nextWithoutConstraint() throws IOException {
    long minTime = timeHeap.pollFirst();
    RowRecord rowRecord = new RowRecord(minTime);

    try {
      for (LayerPointReader reader : transformers) {
        if (!reader.next() || reader.currentTime() != minTime) {
          rowRecord.addField(null);
          continue;
        }
        if (reader.isCurrentNull()) {
          rowRecord.addField(null);
        } else {
          Object value;
          switch (reader.getDataType()) {
            case INT32:
              value = reader.currentInt();
              break;
            case INT64:
              value = reader.currentLong();
              break;
            case FLOAT:
              value = reader.currentFloat();
              break;
            case DOUBLE:
              value = reader.currentDouble();
              break;
            case BOOLEAN:
              value = reader.currentBoolean();
              break;
            case TEXT:
              value = reader.currentBinary();
              break;
            default:
              throw new UnSupportedDataTypeException("Unsupported data type.");
          }
          rowRecord.addField(value, reader.getDataType());
        }
        reader.readyForNext();

        iterateReaderToNextValid(reader);
      }
    } catch (QueryProcessException e) {
      throw new IOException(e.getMessage());
    }

    queryDataSetInputLayer.updateRowRecordListEvictionUpperBound();

    return rowRecord;
  }

  private void iterateReaderToNextValid(LayerPointReader reader)
      throws QueryProcessException, IOException {
    // Since a constant operand is not allowed to be a result column, the reader will not be
    // a ConstantLayerPointReader.
    // If keepNull is false, we must iterate the reader until a non-null row is returned.
    while (reader.next()) {
      if (reader.isCurrentNull() && !keepNull) {
        reader.readyForNext();
        continue;
      }
      timeHeap.add(reader.currentTime());
      break;
    }
  }
}
