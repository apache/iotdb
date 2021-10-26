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
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

// Notice : methods in this class may not be accurate.
public class MemUtils {

  private static Logger logger = LoggerFactory.getLogger(MemUtils.class);

  private MemUtils() {}

  /**
   * function for getting the value size. If mem control enabled, do not add text data size here,
   * the size will be added to memtable before inserting.
   */
  public static long getRecordSize(TSDataType dataType, Object value, boolean addingTextDataSize) {
    if (dataType == TSDataType.TEXT) {
      return 8L + (addingTextDataSize ? getBinarySize((Binary) value) : 0);
    }
    return 8L + dataType.getDataTypeSize();
  }

  /**
   * function for getting the vector value size. If mem control enabled, do not add text data size
   * here, the size will be added to memtable before inserting.
   */
  public static long getVectorRecordSize(
      List<TSDataType> dataTypes, Object[] value, boolean addingTextDataSize) {
    // time and index size
    long memSize = 8L + 4L;
    for (int i = 0; i < dataTypes.size(); i++) {
      if (dataTypes.get(i) == TSDataType.TEXT) {
        memSize += (addingTextDataSize ? getBinarySize((Binary) value[i]) : 0);
      } else {
        memSize += dataTypes.get(i).getDataTypeSize();
      }
    }
    return memSize;
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
  @SuppressWarnings("squid:S3776") // Suppress high Cognitive Complexity warning
  public static long getRecordSize(
      InsertTabletPlan insertTabletPlan, int start, int end, boolean addingTextDataSize) {
    if (insertTabletPlan.getMeasurementMNodes() == null) {
      return getRecordSizeForTest(insertTabletPlan, start, end, addingTextDataSize);
    }
    if (start >= end) {
      return 0L;
    }
    long memSize = 0;
    boolean hasVector = false;
    for (int i = 0; i < insertTabletPlan.getMeasurementMNodes().length; i++) {
      if (insertTabletPlan.getMeasurementMNodes()[i] == null) {
        continue;
      }
      IMeasurementSchema schema = insertTabletPlan.getMeasurementMNodes()[i].getSchema();
      TSDataType valueType;
      if (insertTabletPlan.isAligned()) {
        hasVector = true;
        // value columns memSize
        valueType = schema.getSubMeasurementsTSDataTypeList().get(i);
      } else {
        // time column memSize
        memSize += (end - start) * 8L;
        valueType = insertTabletPlan.getDataTypes()[i];
      }
      if (valueType == TSDataType.TEXT && addingTextDataSize) {
        for (int j = start; j < end; j++) {
          memSize += getBinarySize(((Binary[]) insertTabletPlan.getColumns()[i])[j]);
        }
      } else {
        memSize += (long) (end - start) * valueType.getDataTypeSize();
      }
    }
    // time and index column memSize for vector
    memSize += hasVector ? (end - start) * (8L + 4L) : 0L;
    return memSize;
  }

  /**
   * This method is for test only. This reason is the InsertTabletPlan in tests may doesn't have
   * MeasurementMNodes
   */
  public static long getRecordSizeForTest(
      InsertTabletPlan insertTabletPlan, int start, int end, boolean addingTextDataSize) {
    if (start >= end) {
      return 0L;
    }
    long memSize = 0;
    for (int i = 0; i < insertTabletPlan.getMeasurements().length; i++) {
      // time column memSize
      memSize += (end - start) * 8L;
      if (insertTabletPlan.getDataTypes()[i] == TSDataType.TEXT && addingTextDataSize) {
        for (int j = start; j < end; j++) {
          memSize += getBinarySize(((Binary[]) insertTabletPlan.getColumns()[i])[j]);
        }
      } else {
        memSize += (end - start) * insertTabletPlan.getDataTypes()[i].getDataTypeSize();
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
