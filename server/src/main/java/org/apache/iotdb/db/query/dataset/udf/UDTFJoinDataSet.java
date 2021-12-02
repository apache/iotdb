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

package org.apache.iotdb.db.query.dataset.udf;

import org.apache.iotdb.db.query.dataset.DirectAlignByTimeDataSet;
import org.apache.iotdb.db.tools.watermark.WatermarkEncoder;
import org.apache.iotdb.db.utils.QueryDataSetUtils;
import org.apache.iotdb.db.utils.datastructure.TimeSelector;
import org.apache.iotdb.service.rpc.thrift.TSQueryDataSet;
import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Field;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;
import org.apache.iotdb.tsfile.utils.BytesUtils;
import org.apache.iotdb.tsfile.utils.PublicBAOS;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.IOException;

public class UDTFJoinDataSet extends QueryDataSet implements DirectAlignByTimeDataSet {

  private final UDTFFragmentDataSet[] fragmentDataSets;

  /**
   * Each output column of the UDTFJoinDataSet corresponds to a two-tuple ({@code int[]}) instance
   * in queryDataSetOutputIndexToFragmentDataSetOutputIndex, through the two-tuple instance, the
   * dataset can get the corresponding output column in {@link UDTFJoinDataSet#fragmentDataSets}.
   *
   * <p>About the two-tuple:
   *
   * <p>The first element is the index of the fragmentDataSet which outputs the column.
   *
   * <p>The second element is the index of the actual output column of the given fragmentDataSet.
   */
  private final int[][] resultColumnOutputIndexToFragmentDataSetOutputIndex;

  private final int resultColumnsLength;
  private final RowRecord[] rowRecordsCache;
  private TimeSelector timeHeap;

  public UDTFJoinDataSet(
      UDTFAlignByTimeDataSet udtfAlignByTimeDataSet,
      UDTFFragmentDataSet[] fragmentDataSets,
      int[][] resultColumnOutputIndexToFragmentDataSetOutputIndex)
      throws IOException {
    super(udtfAlignByTimeDataSet);

    this.fragmentDataSets = fragmentDataSets;
    this.resultColumnOutputIndexToFragmentDataSetOutputIndex =
        resultColumnOutputIndexToFragmentDataSetOutputIndex;
    resultColumnsLength = resultColumnOutputIndexToFragmentDataSetOutputIndex.length;
    rowRecordsCache = new RowRecord[resultColumnsLength];

    initTimeHeap();
  }

  private void initTimeHeap() throws IOException {
    timeHeap = new TimeSelector(resultColumnsLength << 1, true);

    for (int i = 0, n = fragmentDataSets.length; i < n; ++i) {
      QueryDataSet fragmentDataSet = fragmentDataSets[i];
      if (fragmentDataSet.hasNextWithoutConstraint()) {
        rowRecordsCache[i] = fragmentDataSet.nextWithoutConstraint();
        timeHeap.add(rowRecordsCache[i].getTimestamp());
      }
    }
  }

  @Override
  public TSQueryDataSet fillBuffer(int fetchSize, WatermarkEncoder encoder) throws IOException {
    TSQueryDataSet tsQueryDataSet = new TSQueryDataSet();

    PublicBAOS timeBAOS = new PublicBAOS();
    PublicBAOS[] valueBAOSList = new PublicBAOS[resultColumnsLength];
    PublicBAOS[] bitmapBAOSList = new PublicBAOS[resultColumnsLength];
    for (int i = 0; i < resultColumnsLength; ++i) {
      valueBAOSList[i] = new PublicBAOS();
      bitmapBAOSList[i] = new PublicBAOS();
    }
    int[] currentBitmapList = new int[resultColumnsLength];

    int rowCount = 0;
    while (rowCount < fetchSize
        && (rowLimit <= 0 || alreadyReturnedRowNum < rowLimit)
        && !timeHeap.isEmpty()) {

      long minTime = timeHeap.pollFirst();
      if (rowOffset == 0) {
        timeBAOS.write(BytesUtils.longToBytes(minTime));
      }

      for (int i = 0; i < resultColumnsLength; ++i) {
        int[] indexes = resultColumnOutputIndexToFragmentDataSetOutputIndex[i];
        int fragmentDataSetIndex = indexes[0];
        int outputColumnIndexInFragmentDataSet = indexes[1];

        if (rowRecordsCache[fragmentDataSetIndex] == null) {
          if (rowOffset == 0) {
            currentBitmapList[i] = (currentBitmapList[i] << 1);
          }
          continue;
        }

        RowRecord fragmentRowRecord = rowRecordsCache[fragmentDataSetIndex];
        if (fragmentRowRecord.getTimestamp() != minTime) {
          if (rowOffset == 0) {
            currentBitmapList[i] = (currentBitmapList[i] << 1);
          }
          continue;
        }

        Field field = fragmentRowRecord.getFields().get(outputColumnIndexInFragmentDataSet);
        if (field == null || field.getDataType() == null) {
          if (rowOffset == 0) {
            currentBitmapList[i] = (currentBitmapList[i] << 1);
          }
          continue;
        }

        if (rowOffset == 0) {
          currentBitmapList[i] = (currentBitmapList[i] << 1) | FLAG;

          TSDataType type = field.getDataType();
          switch (type) {
            case INT32:
              int intValue = field.getIntV();
              ReadWriteIOUtils.write(
                  encoder != null && encoder.needEncode(minTime)
                      ? encoder.encodeInt(intValue, minTime)
                      : intValue,
                  valueBAOSList[i]);
              break;
            case INT64:
              long longValue = field.getLongV();
              ReadWriteIOUtils.write(
                  encoder != null && encoder.needEncode(minTime)
                      ? encoder.encodeLong(longValue, minTime)
                      : longValue,
                  valueBAOSList[i]);
              break;
            case FLOAT:
              float floatValue = field.getFloatV();
              ReadWriteIOUtils.write(
                  encoder != null && encoder.needEncode(minTime)
                      ? encoder.encodeFloat(floatValue, minTime)
                      : floatValue,
                  valueBAOSList[i]);
              break;
            case DOUBLE:
              double doubleValue = field.getDoubleV();
              ReadWriteIOUtils.write(
                  encoder != null && encoder.needEncode(minTime)
                      ? encoder.encodeDouble(doubleValue, minTime)
                      : doubleValue,
                  valueBAOSList[i]);
              break;
            case BOOLEAN:
              ReadWriteIOUtils.write(field.getBoolV(), valueBAOSList[i]);
              break;
            case TEXT:
              ReadWriteIOUtils.write(field.getBinaryV(), valueBAOSList[i]);
              break;
            default:
              throw new UnSupportedDataTypeException(
                  String.format("Data type %s is not supported.", type));
          }
        }
      }

      updateRowRecordsCache(minTime);

      if (rowOffset == 0) {
        ++rowCount;
        if (rowCount % 8 == 0) {
          for (int i = 0; i < resultColumnsLength; ++i) {
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
        for (int i = 0; i < resultColumnsLength; ++i) {
          ReadWriteIOUtils.write(
              (byte) (currentBitmapList[i] << (8 - remaining)), bitmapBAOSList[i]);
        }
      }
    }

    return QueryDataSetUtils.packBuffer(
        tsQueryDataSet, timeBAOS, valueBAOSList, bitmapBAOSList, resultColumnsLength);
  }

  @Override
  public boolean hasNextWithoutConstraint() throws IOException {
    return !timeHeap.isEmpty();
  }

  @Override
  public RowRecord nextWithoutConstraint() throws IOException {
    long minTime = timeHeap.pollFirst();
    RowRecord rowRecord = new RowRecord(minTime, resultColumnsLength);

    for (int i = 0; i < resultColumnsLength; ++i) {
      int[] indexes = resultColumnOutputIndexToFragmentDataSetOutputIndex[i];
      int fragmentDataSetIndex = indexes[0];
      int outputColumnIndexInFragmentDataSet = indexes[1];

      if (rowRecordsCache[fragmentDataSetIndex] == null) {
        rowRecord.addField(null);
        continue;
      }

      RowRecord fragmentRowRecord = rowRecordsCache[fragmentDataSetIndex];
      if (fragmentRowRecord.getTimestamp() != minTime) {
        rowRecord.addField(null);
        continue;
      }

      rowRecord.addField(fragmentRowRecord.getFields().get(outputColumnIndexInFragmentDataSet));
    }

    updateRowRecordsCache(minTime);

    return rowRecord;
  }

  private void updateRowRecordsCache(long minTime) {
    for (int i = 0, n = fragmentDataSets.length; i < n; ++i) {
      if (rowRecordsCache[i] == null) {
        continue;
      }

      RowRecord fragmentRowRecord = rowRecordsCache[i];
      if (fragmentRowRecord.getTimestamp() != minTime) {
        continue;
      }

      rowRecordsCache[i] = null;

      if (fragmentDataSets[i].hasNextWithoutConstraint()) {
        fragmentRowRecord = fragmentDataSets[i].nextWithoutConstraint();
        rowRecordsCache[i] = fragmentRowRecord;
        timeHeap.add(fragmentRowRecord.getTimestamp());
      }
    }
  }
}
