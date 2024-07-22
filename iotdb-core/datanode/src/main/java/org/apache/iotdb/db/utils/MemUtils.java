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

package org.apache.iotdb.db.utils;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.schema.table.column.TsTableColumnCategory;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertTabletNode;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.RamUsageEstimator;
import org.apache.tsfile.write.record.TSRecord;
import org.apache.tsfile.write.record.datapoint.BooleanDataPoint;
import org.apache.tsfile.write.record.datapoint.DataPoint;
import org.apache.tsfile.write.record.datapoint.DoubleDataPoint;
import org.apache.tsfile.write.record.datapoint.FloatDataPoint;
import org.apache.tsfile.write.record.datapoint.IntDataPoint;
import org.apache.tsfile.write.record.datapoint.LongDataPoint;
import org.apache.tsfile.write.record.datapoint.StringDataPoint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

// Notice : methods in this class may not be accurate.
public class MemUtils {

  private static final Logger LOGGER = LoggerFactory.getLogger(MemUtils.class);

  private MemUtils() {}

  /**
   * Function for obtaining the value size. For text values, there are two conditions: 1. During
   * insertion, their size has already been added to memory. 2. During flushing, their size needs to
   * be calculated.
   */
  public static long getRecordSize(TSDataType dataType, Object value, boolean addingTextDataSize) {
    if (dataType.isBinary()) {
      return 8L + (addingTextDataSize ? getBinarySize((Binary) value) : 0);
    }
    return 8L + dataType.getDataTypeSize();
  }

  /**
   * Function for obtaining the value size. For text values, their size has already been added to
   * memory before insertion
   */
  public static long getRowRecordSize(List<TSDataType> dataTypes, Object[] value) {
    int emptyRecordCount = 0;
    long memSize = 0L;
    for (int i = 0; i < value.length; i++) {
      if (value[i] == null) {
        emptyRecordCount++;
        continue;
      }
      memSize += getRecordSize(dataTypes.get(i - emptyRecordCount), value[i], false);
    }
    return memSize;
  }

  /**
   * Function for obtaining the value size. For text values, their size has already been added to
   * memory before insertion
   */
  public static long getAlignedRowRecordSize(
      List<TSDataType> dataTypes, Object[] value, TsTableColumnCategory[] columnCategories) {
    // time and index size
    long memSize = 8L + 4L;
    for (int i = 0; i < dataTypes.size(); i++) {
      if (value[i] == null
          || dataTypes.get(i).isBinary()
          || columnCategories != null && columnCategories[i] != TsTableColumnCategory.MEASUREMENT) {
        continue;
      }
      memSize += dataTypes.get(i).getDataTypeSize();
    }
    return memSize;
  }

  public static long getBinarySize(Binary value) {
    return RamUsageEstimator.NUM_BYTES_OBJECT_HEADER + RamUsageEstimator.sizeOf(value.getValues());
  }

  public static long getBinaryColumnSize(Binary[] column, int start, int end, TSStatus[] results) {
    long memSize = 0;
    memSize += (long) (end - start) * RamUsageEstimator.NUM_BYTES_OBJECT_HEADER;
    for (int i = start; i < end; i++) {
      if (results == null
          || results[i] == null
          || results[i].code == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        memSize += RamUsageEstimator.sizeOf(column[i].getValues());
      }
    }
    return memSize;
  }

  /**
   * Function for obtaining the value size. For text values, their size has already been added to
   * memory before insertion
   */
  public static long getTabletSize(InsertTabletNode insertTabletNode, int start, int end) {
    if (start >= end) {
      return 0L;
    }
    long memSize = 0;
    for (int i = 0; i < insertTabletNode.getMeasurements().length; i++) {
      if (insertTabletNode.getMeasurements()[i] == null) {
        continue;
      }
      // Time column memSize
      memSize += (end - start) * 8L;
      memSize += (long) (end - start) * insertTabletNode.getDataTypes()[i].getDataTypeSize();
    }
    return memSize;
  }

  public static long getAlignedTabletSize(
      InsertTabletNode insertTabletNode, int start, int end, TSStatus[] results) {
    if (start >= end) {
      return 0L;
    }
    long memSize = 0;
    for (int i = 0; i < insertTabletNode.getMeasurements().length; i++) {
      if (!insertTabletNode.isValidMeasurement(i)) {
        continue;
      }
      if (results == null) {
        memSize += (long) (end - start) * insertTabletNode.getDataTypes()[i].getDataTypeSize();
      } else {
        for (int j = start; j < end; j++) {
          if (results[j] == null
              || results[j].code == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
            memSize += insertTabletNode.getDataTypes()[i].getDataTypeSize();
          }
        }
      }
    }
    // time and index column memSize for vector
    memSize += (end - start) * (8L + 4L);
    return memSize;
  }

  /** Calculate how much memory will be used if the given record is written to sequence file. */
  public static long getTsRecordMem(TSRecord tsRecord) {
    long memUsed = 8; // time
    memUsed += 8; // deviceId reference
    memUsed += tsRecord.deviceId.ramBytesUsed();
    for (DataPoint dataPoint : tsRecord.dataPointList) {
      memUsed += 8; // dataPoint reference
      memUsed += getDataPointMem(dataPoint);
    }
    return memUsed;
  }

  /** Function for getting the memory size of the given string. */
  public static long getStringMem(String str) {
    // wide char (2 bytes each) and 64B String overhead
    return str.length() * 2L + 64L;
  }

  /** Function for getting the memory size of the given data point. */
  public static long getDataPointMem(DataPoint dataPoint) {
    // type reference
    long memUsed = 8;
    // measurementId and its reference
    memUsed += getStringMem(dataPoint.getMeasurementId());
    memUsed += 8;

    if (dataPoint instanceof FloatDataPoint) {
      memUsed += 4;
    } else if (dataPoint instanceof IntDataPoint) {
      memUsed += 4;
    } else if (dataPoint instanceof BooleanDataPoint) {
      memUsed += 1;
    } else if (dataPoint instanceof DoubleDataPoint) {
      memUsed += 8;
    } else if (dataPoint instanceof LongDataPoint) {
      memUsed += 8;
    } else if (dataPoint instanceof StringDataPoint) {
      StringDataPoint stringDataPoint = (StringDataPoint) dataPoint;
      memUsed += 8 + 20; // array reference and array overhead
      memUsed += ((Binary) stringDataPoint.getValue()).getLength();
      // encoding string reference and its memory
      memUsed += 8;
      memUsed += getStringMem(TSFileConfig.STRING_ENCODING);
    } else {
      LOGGER.error("Unsupported data point type");
    }

    return memUsed;
  }

  /** Function for converting the byte count result to readable string. */
  public static String bytesCntToStr(long inputCnt) {
    long cnt = inputCnt;
    long gbs = cnt / IoTDBConstant.GB;
    cnt = cnt % IoTDBConstant.GB;
    long mbs = cnt / IoTDBConstant.MB;
    cnt = cnt % IoTDBConstant.MB;
    long kbs = cnt / IoTDBConstant.KB;
    cnt = cnt % IoTDBConstant.KB;
    return gbs + " GB " + mbs + " MB " + kbs + " KB " + cnt + " B";
  }

  public static long strToBytesCnt(String str) {
    if (str == null || str.isEmpty()) {
      return 0;
    }
    str = str.toLowerCase();
    if (!str.endsWith("b")) {
      str += "b";
    }
    long unit = 1;
    if (str.endsWith("kb")) {
      unit = IoTDBConstant.KB;
    } else if (str.endsWith("mb")) {
      unit = IoTDBConstant.MB;
    } else if (str.endsWith("gb")) {
      unit = IoTDBConstant.GB;
    }
    str = str.replaceAll("\\D", "");
    return Long.parseLong(str) * unit;
  }
}
