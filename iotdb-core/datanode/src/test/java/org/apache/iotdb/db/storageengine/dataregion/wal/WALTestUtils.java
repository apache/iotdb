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
package org.apache.iotdb.db.storageengine.dataregion.wal;

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertRowNode;

import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.write.schema.MeasurementSchema;

import java.lang.reflect.Field;

public class WALTestUtils {
  public static void setMinCompressionSize(long size)
      throws NoSuchFieldException, ClassNotFoundException, IllegalAccessException {
    Class<?> logWriterClass =
        Class.forName("org.apache.iotdb.db.storageengine.dataregion.wal.io.LogWriter");
    Field minCompressionSizeField = logWriterClass.getDeclaredField("MIN_COMPRESSION_SIZE");
    minCompressionSizeField.setAccessible(true);
    minCompressionSizeField.setLong(null, size);
  }

  public static long getMinCompressionSize()
      throws ClassNotFoundException, NoSuchFieldException, IllegalAccessException {
    Class<?> logWriterClass =
        Class.forName("org.apache.iotdb.db.storageengine.dataregion.wal.io.LogWriter");
    Field minCompressionSizeField = logWriterClass.getDeclaredField("MIN_COMPRESSION_SIZE");
    minCompressionSizeField.setAccessible(true);
    return minCompressionSizeField.getLong(null);
  }

  public static InsertRowNode getInsertRowNode(String devicePath, long time)
      throws IllegalPathException, QueryProcessException {
    TSDataType[] dataTypes =
        new TSDataType[] {
          TSDataType.DOUBLE,
          TSDataType.FLOAT,
          TSDataType.INT64,
          TSDataType.INT32,
          TSDataType.BOOLEAN,
          TSDataType.TEXT
        };

    Object[] columns = new Object[6];
    columns[0] = 1.0d;
    columns[1] = 2f;
    columns[2] = 10000L;
    columns[3] = 100;
    columns[4] = false;
    columns[5] = new Binary("hh" + 0, TSFileConfig.STRING_CHARSET);

    InsertRowNode node =
        new InsertRowNode(
            new PlanNodeId(""),
            new PartialPath(devicePath),
            false,
            new String[] {"s1", "s2", "s3", "s4", "s5", "s6"},
            dataTypes,
            time,
            columns,
            false);
    MeasurementSchema[] schemas = new MeasurementSchema[6];
    for (int i = 0; i < 6; i++) {
      schemas[i] = new MeasurementSchema("s" + (i + 1), dataTypes[i]);
    }
    node.setMeasurementSchemas(schemas);
    return node;
  }
}
