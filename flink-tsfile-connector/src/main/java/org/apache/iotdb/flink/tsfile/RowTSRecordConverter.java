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

package org.apache.iotdb.flink.tsfile;

import org.apache.iotdb.tsfile.common.constant.QueryConstant;
import org.apache.iotdb.tsfile.common.constant.TsFileConstant;
import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.write.record.TSRecord;
import org.apache.iotdb.tsfile.write.record.datapoint.BooleanDataPoint;
import org.apache.iotdb.tsfile.write.record.datapoint.DataPoint;
import org.apache.iotdb.tsfile.write.record.datapoint.DoubleDataPoint;
import org.apache.iotdb.tsfile.write.record.datapoint.FloatDataPoint;
import org.apache.iotdb.tsfile.write.record.datapoint.IntDataPoint;
import org.apache.iotdb.tsfile.write.record.datapoint.LongDataPoint;
import org.apache.iotdb.tsfile.write.record.datapoint.StringDataPoint;
import org.apache.iotdb.tsfile.write.schema.Schema;

import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/** The converter that convert a Row object to multiple TSRecord objects. */
public class RowTSRecordConverter implements TSRecordConverter<Row> {

  private RowTypeInfo rowTypeInfo;
  private transient TSRecord[] outputTemplate;
  private transient int timeIndex = -1;
  private transient int[] tsRecordIndexMapping;
  private transient int[] dataPointIndexMapping;
  private transient TSRecord[] reuse;

  public RowTSRecordConverter(RowTypeInfo rowTypeInfo) {
    this.rowTypeInfo = rowTypeInfo;
  }

  @Override
  public void open(Schema schema) {
    this.tsRecordIndexMapping = new int[rowTypeInfo.getArity()];
    this.dataPointIndexMapping = new int[rowTypeInfo.getArity()];
    List<TSRecord> outputTemplateList = new ArrayList<>();

    for (int i = 0; i < rowTypeInfo.getArity(); i++) {
      String fieldName = rowTypeInfo.getFieldNames()[i];
      if (QueryConstant.RESERVED_TIME.equals(fieldName)) {
        timeIndex = i;
        tsRecordIndexMapping[i] = -1;
        dataPointIndexMapping[i] = -1;
        continue;
      }
      String deviceId =
          fieldName.substring(0, fieldName.lastIndexOf(TsFileConstant.PATH_SEPARATOR));
      String measurementId =
          fieldName.substring(fieldName.lastIndexOf(TsFileConstant.PATH_SEPARATOR) + 1);
      int tsRecordIndex =
          outputTemplateList.stream()
              .map(t -> t.deviceId)
              .collect(Collectors.toList())
              .indexOf(deviceId);
      if (tsRecordIndex < 0) {
        outputTemplateList.add(new TSRecord(0, deviceId));
        tsRecordIndex = outputTemplateList.size() - 1;
      }
      tsRecordIndexMapping[i] = tsRecordIndex;
      TSRecord tsRecord = outputTemplateList.get(tsRecordIndex);
      Class typeClass = rowTypeInfo.getFieldTypes()[i].getTypeClass();
      if (typeClass == Boolean.class || typeClass == boolean.class) {
        tsRecord.addTuple(new BooleanDataPoint(measurementId, false));
      } else if (typeClass == Integer.class || typeClass == int.class) {
        tsRecord.addTuple(new IntDataPoint(measurementId, 0));
      } else if (typeClass == Long.class || typeClass == long.class) {
        tsRecord.addTuple(new LongDataPoint(measurementId, 0));
      } else if (typeClass == Float.class || typeClass == float.class) {
        tsRecord.addTuple(new FloatDataPoint(measurementId, 0));
      } else if (typeClass == Double.class || typeClass == double.class) {
        tsRecord.addTuple(new DoubleDataPoint(measurementId, 0));
      } else if (typeClass == String.class) {
        tsRecord.addTuple(new StringDataPoint(measurementId, null));
      } else {
        throw new UnSupportedDataTypeException(typeClass.toString());
      }
      dataPointIndexMapping[i] = tsRecord.dataPointList.size() - 1;
    }
    outputTemplate = outputTemplateList.toArray(new TSRecord[0]);

    reuse = new TSRecord[outputTemplate.length];
    for (int i = 0; i < outputTemplate.length; i++) {
      reuse[i] = new TSRecord(0, outputTemplate[i].deviceId);
    }
  }

  @Override
  public void convert(Row input, Collector<TSRecord> collector) {
    long timestamp = (long) input.getField(timeIndex);
    for (TSRecord tsRecord : reuse) {
      tsRecord.dataPointList.clear();
    }
    for (int i = 0; i < input.getArity(); i++) {
      if (i == timeIndex) {
        continue;
      }
      TSRecord templateRecord = outputTemplate[tsRecordIndexMapping[i]];
      DataPoint templateDataPoint = templateRecord.dataPointList.get(dataPointIndexMapping[i]);
      Object o = input.getField(i);
      if (o != null) {
        switch (templateDataPoint.getType()) {
          case BOOLEAN:
            templateDataPoint.setBoolean((Boolean) o);
            break;
          case INT32:
            templateDataPoint.setInteger((Integer) o);
            break;
          case INT64:
            templateDataPoint.setLong((Long) o);
            break;
          case FLOAT:
            templateDataPoint.setFloat((Float) o);
            break;
          case DOUBLE:
            templateDataPoint.setDouble((Double) o);
            break;
          case TEXT:
            templateDataPoint.setString(Binary.valueOf((String) o));
            break;
          default:
            templateDataPoint.setString(Binary.valueOf(o.toString()));
        }
        reuse[tsRecordIndexMapping[i]].addTuple(templateDataPoint);
      }
    }
    for (TSRecord tsRecord : reuse) {
      if (tsRecord.dataPointList.size() > 0) {
        tsRecord.setTime(timestamp);
        collector.collect(tsRecord);
      }
    }
  }

  @Override
  public void close() {
    outputTemplate = null;
    timeIndex = -1;
    tsRecordIndexMapping = null;
    dataPointIndexMapping = null;
  }
}
