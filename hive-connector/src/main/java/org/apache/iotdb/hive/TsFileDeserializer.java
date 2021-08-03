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
package org.apache.iotdb.hive;

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class TsFileDeserializer {
  private static final Logger LOG = LoggerFactory.getLogger(TsFileDeserializer.class);
  private static final String ERROR_MSG = "Unexpected data type: %s for Date TypeInfo: %s";
  private List<Object> row;

  /**
   * Deserialize an TsFile record, recursing into its component fields and deserializing them as
   * well. Fields of the record are matched by name against fields in the Hive row.
   *
   * <p>Because TsFile has some data types that Hive does not, these are converted during
   * deserialization to types Hive will work with.
   *
   * @param columnNames List of columns Hive is expecting from record.
   * @param columnTypes List of column types matched by index to names
   * @param writable Instance of MapWritable to deserialize
   * @param deviceId device name
   * @return A list of objects suitable for Hive to work with further
   * @throws TsFileSerDeException For any exception during deserialization
   */
  @SuppressWarnings("squid:S3776") // Suppress high Cognitive Complexity warning
  public Object deserialize(
      List<String> columnNames, List<TypeInfo> columnTypes, Writable writable, String deviceId)
      throws TsFileSerDeException {
    if (!(writable instanceof MapWritable)) {
      throw new TsFileSerDeException("Expecting a MapWritable");
    }

    if (row == null || row.size() != columnNames.size()) {
      row = new ArrayList<>(columnNames.size());
    } else {
      row.clear();
    }
    MapWritable mapWritable = (MapWritable) writable;
    if (!Objects.equals(mapWritable.get(new Text("device_id")).toString(), deviceId)) {
      return null;
    }

    LOG.debug("device_id: {}", mapWritable.get(new Text("device_id")));
    LOG.debug("time_stamp: {}", mapWritable.get(new Text("time_stamp")));

    for (int i = 0; i < columnNames.size(); i++) {
      TypeInfo columnType = columnTypes.get(i);
      String columnName = columnNames.get(i);
      Writable data = mapWritable.get(new Text(columnName));
      if (data == null || data instanceof NullWritable) {
        row.add(null);
        continue;
      }
      if (columnType.getCategory() != ObjectInspector.Category.PRIMITIVE) {
        throw new TsFileSerDeException("Unknown TypeInfo: " + columnType.getCategory());
      }
      PrimitiveObjectInspector.PrimitiveCategory type =
          ((PrimitiveTypeInfo) columnType).getPrimitiveCategory();
      switch (type) {
        case BOOLEAN:
          if (data instanceof BooleanWritable) {
            row.add(((BooleanWritable) data).get());
          } else {
            throw new TsFileSerDeException(
                String.format(ERROR_MSG, data.getClass().getName(), type));
          }

          break;
        case INT:
          if (data instanceof IntWritable) {
            row.add(((IntWritable) data).get());
          } else {
            throw new TsFileSerDeException(
                String.format(ERROR_MSG, data.getClass().getName(), type));
          }
          break;
        case LONG:
          if (data instanceof LongWritable) {
            row.add(((LongWritable) data).get());
          } else {
            throw new TsFileSerDeException(
                String.format(ERROR_MSG, data.getClass().getName(), type));
          }
          break;
        case FLOAT:
          if (data instanceof FloatWritable) {
            row.add(((FloatWritable) data).get());
          } else {
            throw new TsFileSerDeException(
                String.format(ERROR_MSG, data.getClass().getName(), type));
          }
          break;
        case DOUBLE:
          if (data instanceof DoubleWritable) {
            row.add(((DoubleWritable) data).get());
          } else {
            throw new TsFileSerDeException(
                String.format(ERROR_MSG, data.getClass().getName(), type));
          }
          break;
        case STRING:
          if (data instanceof Text) {
            row.add(data.toString());
          } else {
            throw new TsFileSerDeException(
                String.format(ERROR_MSG, data.getClass().getName(), type));
          }
          break;
        case TIMESTAMP:
          if (data instanceof LongWritable) {
            row.add(new Timestamp(((LongWritable) data).get()));
          } else {
            throw new TsFileSerDeException(
                String.format(ERROR_MSG, data.getClass().getName(), type));
          }
          break;
        default:
          throw new TsFileSerDeException("Unknown TypeInfo: " + columnType.getCategory());
      }
    }
    return row;
  }
}
