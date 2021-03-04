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
import org.apache.iotdb.tsfile.read.common.Field;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.common.RowRecord;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.types.Row;

import java.util.List;
import java.util.stream.Collectors;

/** The parser that parses the RowRecord objects read from TsFile into Flink Row object. */
public class RowRowRecordParser implements RowRecordParser<Row>, ResultTypeQueryable<Row> {

  private final int[] indexMapping;
  private final RowTypeInfo rowTypeInfo;

  public RowRowRecordParser(int[] indexMapping, RowTypeInfo rowTypeInfo) {
    this.indexMapping = indexMapping;
    this.rowTypeInfo = rowTypeInfo;
  }

  @Override
  public Row parse(RowRecord rowRecord, Row reuse) {
    List<Field> fields = rowRecord.getFields();
    for (int i = 0; i < indexMapping.length; i++) {
      if (indexMapping[i] < 0) {
        // The negative index is treated as the marker of timestamp.
        reuse.setField(i, rowRecord.getTimestamp());
      } else {
        reuse.setField(i, toSqlValue(fields.get(indexMapping[i])));
      }
    }
    return reuse;
  }

  private Object toSqlValue(Field field) {
    if (field == null) {
      return null;
    } else if (field.getDataType() == null) {
      return null;
    } else {
      switch (field.getDataType()) {
        case BOOLEAN:
          return field.getBoolV();
        case INT32:
          return field.getIntV();
        case INT64:
          return field.getLongV();
        case FLOAT:
          return field.getFloatV();
        case DOUBLE:
          return field.getDoubleV();
        case TEXT:
          return field.getStringValue();
        default:
          throw new UnsupportedOperationException(
              String.format("Unsupported type %s", field.getDataType()));
      }
    }
  }

  @Override
  public TypeInformation<Row> getProducedType() {
    return rowTypeInfo;
  }

  /**
   * Creates RowRowRecordParser from output RowTypeInfo and selected series in the RowRecord. The
   * row field "time" will be used to store the timestamp value. The other row fields store the
   * values of the same field names of the RowRecord.
   *
   * @param outputRowTypeInfo The RowTypeInfo of the output row.
   * @param selectedSeries The selected series in the RowRecord.
   * @return The RowRowRecordParser.
   */
  public static RowRowRecordParser create(
      RowTypeInfo outputRowTypeInfo, List<Path> selectedSeries) {
    List<String> selectedSeriesNames =
        selectedSeries.stream().map(Path::toString).collect(Collectors.toList());
    String[] rowFieldNames = outputRowTypeInfo.getFieldNames();
    int[] indexMapping = new int[outputRowTypeInfo.getArity()];
    for (int i = 0; i < outputRowTypeInfo.getArity(); i++) {
      if (!QueryConstant.RESERVED_TIME.equals(rowFieldNames[i])) {
        int index = selectedSeriesNames.indexOf(rowFieldNames[i]);
        if (index >= 0) {
          indexMapping[i] = index;
        } else {
          throw new IllegalArgumentException(
              rowFieldNames[i] + " is not found in selected series.");
        }
      } else {
        // marked as timestamp field.
        indexMapping[i] = -1;
      }
    }
    return new RowRowRecordParser(indexMapping, outputRowTypeInfo);
  }
}
