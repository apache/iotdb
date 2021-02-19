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

import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.qp.physical.crud.InsertTabletPlan;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.utils.RamUsageEstimator;
import org.apache.iotdb.tsfile.write.record.TSRecord;
import org.apache.iotdb.tsfile.write.record.datapoint.BooleanDataPoint;
import org.apache.iotdb.tsfile.write.record.datapoint.DataPoint;
import org.apache.iotdb.tsfile.write.record.datapoint.DoubleDataPoint;
import org.apache.iotdb.tsfile.write.record.datapoint.FloatDataPoint;
import org.apache.iotdb.tsfile.write.record.datapoint.IntDataPoint;
import org.apache.iotdb.tsfile.write.record.datapoint.LongDataPoint;
import org.apache.iotdb.tsfile.write.record.datapoint.StringDataPoint;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// Notice : methods in this class may not be accurate.
public class MemUtils {

  private static Logger logger = LoggerFactory.getLogger(MemUtils.class);

  private MemUtils() {}

  /**
   * function for getting the value size. If mem control enabled, do not add text data size here,
   * the size will be added to memtable before inserting.
   */
  public static long getRecordSize(TSDataType dataType, Object value, boolean addingTextDataSize) {
    switch (dataType) {
      case INT32:
        return 8L + 4L;
      case INT64:
        return 8L + 8L;
      case FLOAT:
        return 8L + 4L;
      case DOUBLE:
        return 8L + 8L;
      case BOOLEAN:
        return 8L + 1L;
      case TEXT:
        return 8L + (addingTextDataSize ? getBinarySize((Binary) value) : 0);
      default:
        return 8L + 8L;
    }
  }

  public static long getBinarySize(Binary value) {
    return RamUsageEstimator.NUM_BYTES_OBJECT_HEADER + RamUsageEstimator.sizeOf(value.getValues());
  }

  public static long getBinaryColumnSize(Binary[] column, int start, int end) {
    long memSize = 0;
    memSize += (end - start) * RamUsageEstimator.NUM_BYTES_OBJECT_HEADER;
    for (int i = start; i < end; i++) {
      memSize += RamUsageEstimator.sizeOf(column[i].getValues());
    }
    return memSize;
  }

  /**
   * If mem control enabled, do not add text data size here, the size will be added to memtable
   * before inserting.
   */
  public static long getRecordSize(
      InsertTabletPlan insertTabletPlan, int start, int end, boolean addingTextDataSize) {
    if (start >= end) {
      return 0L;
    }
    long memSize = 0;
    for (int i = 0; i < insertTabletPlan.getMeasurements().length; i++) {
      if (insertTabletPlan.getDataTypes()[i] == null) {
        continue;
      }
      switch (insertTabletPlan.getDataTypes()[i]) {
        case INT32:
          memSize += (end - start) * (8L + 4L);
          break;
        case INT64:
          memSize += (end - start) * (8L + 8L);
          break;
        case FLOAT:
          memSize += (end - start) * (8L + 4L);
          break;
        case DOUBLE:
          memSize += (end - start) * (8L + 8L);
          break;
        case BOOLEAN:
          memSize += (end - start) * (8L + 1L);
          break;
        case TEXT:
          memSize += (end - start) * 8L;
          if (addingTextDataSize) {
            for (int j = start; j < end; j++) {
              memSize += getBinarySize(((Binary[]) insertTabletPlan.getColumns()[i])[j]);
            }
          }
          break;
        default:
          memSize += (end - start) * (8L + 8L);
      }
    }
    return memSize;
  }

  /** Calculate how much memory will be used if the given record is written to sequence file. */
  public static long getTsRecordMem(TSRecord record) {
    long memUsed = 8; // time
    memUsed += 8; // deviceId reference
    memUsed += getStringMem(record.deviceId);
    for (DataPoint dataPoint : record.dataPointList) {
      memUsed += 8; // dataPoint reference
      memUsed += getDataPointMem(dataPoint);
    }
    return memUsed;
  }

  /** function for getting the memory size of the given string. */
  public static long getStringMem(String str) {
    // wide char (2 bytes each) and 64B String overhead
    return str.length() * 2 + 64L;
  }

  /** function for getting the memory size of the given data point. */
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
      memUsed += getStringMem(((Binary) stringDataPoint.getValue()).getTextEncodingType());
    } else {
      logger.error("Unsupported data point type");
    }

    return memUsed;
  }

  /** function for converting the byte count result to readable string. */
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
}
