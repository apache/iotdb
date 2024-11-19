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

package org.apache.iotdb.db.exp;

import org.apache.iotdb.db.queryengine.transformation.dag.util.TypeUtils;
import org.apache.iotdb.udf.api.access.Row;
import org.apache.iotdb.udf.api.type.Binary;
import org.apache.iotdb.udf.api.type.Type;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.column.LongColumn;

import java.io.IOException;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class ExpScalarFunction {

  void processBatch(Column[] columns, ColumnBuilder builder) {
    int count = columns[0].getPositionCount();
    for (int i = 0; i < count; i++) {
      long sum = 0;
      for (int j = 0; j < columns.length; j++) {
        sum += columns[j].getLong(i);
      }
      builder.writeLong(Math.abs(sum));
    }
  }

  void processSingle(Column[] columns, ColumnBuilder builder) {
    Object[] row = new Object[columns.length];
    for (int i = 0; i < columns[0].getPositionCount(); i++) {
      for (int j = 0; j < columns.length; j++) {
        row[j] = columns[j].getObject(i);
      }
      builder.writeLong((long) processSingle(row));
    }
  }

  Object processSingle(Object[] inputs) {
    long sum = 0;
    for (int i = 0; i < inputs.length; i++) {
      if (inputs[i] != null) {
        sum += (long) inputs[i];
      }
    }
    return Math.abs(sum);
  }

  void processRow(Column[] columns, ColumnBuilder builder) throws IOException {
    for (int i = 0; i < columns[0].getPositionCount(); i++) {
      final int finalI = i;
      builder.writeLong(
          (long)
              processRow(
                  new Row() {
                    @Override
                    public long getTime() throws IOException {
                      return 0;
                    }

                    @Override
                    public int getInt(int columnIndex) throws IOException {
                      return 0;
                    }

                    @Override
                    public long getLong(int columnIndex) throws IOException {
                      return columns[columnIndex].getLong(finalI);
                    }

                    @Override
                    public float getFloat(int columnIndex) throws IOException {
                      return 0;
                    }

                    @Override
                    public double getDouble(int columnIndex) throws IOException {
                      return 0;
                    }

                    @Override
                    public boolean getBoolean(int columnIndex) throws IOException {
                      return false;
                    }

                    @Override
                    public Binary getBinary(int columnIndex) throws IOException {
                      return null;
                    }

                    @Override
                    public String getString(int columnIndex) throws IOException {
                      return "";
                    }

                    @Override
                    public Type getDataType(int columnIndex) {
                      return null;
                    }

                    @Override
                    public boolean isNull(int columnIndex) throws IOException {
                      return false;
                    }

                    @Override
                    public int size() {
                      return columns.length;
                    }
                  }));
    }
  }

  Object processRow(Row row) throws IOException {
    long sum = 0;
    for (int i = 0; i < row.size(); i++) {
      sum += row.getLong(i);
    }
    return Math.abs(sum);
  }

  static void processMethodHandle1(
      Column[] columns, ColumnBuilder builder, MethodHandle processRawHandle) throws Throwable {
    Object[] row = new Object[columns.length];
    for (int i = 0; i < columns[0].getPositionCount(); i++) {
      for (int j = 0; j < columns.length; j++) {
        row[j] = columns[j].getObject(i);
      }
      builder.writeLong((long) processRawHandle.invokeWithArguments(row));
    }
  }

  static void processMethodHandle2(
      Column[] columns, ColumnBuilder builder, MethodHandle processRawHandle) throws Throwable {
    for (int i = 0; i < columns[0].getPositionCount(); i++) {
      long a = columns[0].getLong(i);
      long b = columns[1].getLong(i);
      long c = columns[2].getLong(i);
      long d = columns[3].getLong(i);
      long e = columns[4].getLong(i);
      builder.writeLong((long) processRawHandle.invoke(a, b, c, d, e));
    }
  }

  static long processRaw(long a, long b, long c, long d, long e) {
    return Math.abs(a + b + c + d + e);
  }

  public static void main(String[] args) {
    int COLUMN_COUNT = 5;
    int ROW_COUNT = 50000;
    int LOOP_COUNT = 100;
    int EPOCH = 10;
    ExpScalarFunction expScalarFunction = new ExpScalarFunction();
    // create column[] and columnBuilder
    List<Column[]> columns = new ArrayList<>();
    for (int loop = 0; loop < LOOP_COUNT; loop++) {
      columns.add(new LongColumn[COLUMN_COUNT]);
      for (int i = 0; i < COLUMN_COUNT; i++) {
        long[] values = new long[ROW_COUNT];
        for (int j = 0; j < ROW_COUNT; j++) {
          // random value
          values[j] = (long) (Math.random() * 100);
        }
        columns.get(loop)[i] = new LongColumn(ROW_COUNT, Optional.empty(), values);
      }
    }
    System.out.println("Start testing...");

    try {
      // 获取 processRaw 方法的 MethodHandle
      MethodHandle processRawHandle =
          MethodHandles.lookup()
              .findStatic(
                  ExpScalarFunction.class,
                  "processRaw",
                  MethodType.methodType(
                      long.class, long.class, long.class, long.class, long.class, long.class));
      long startTime = System.currentTimeMillis();
      //            for (int j = 0; j < EPOCH; j++) {
      //                for (int i = 0; i < LOOP_COUNT; i++) {
      //                    ColumnBuilder builder = TypeUtils.initColumnBuilder(TSDataType.INT64,
      // ROW_COUNT);
      //                    try {
      //                        processMethodHandle1(columns.get(i), builder, processRawHandle);
      //                    } catch (Throwable throwable) {
      //                        throwable.printStackTrace();
      //                    }
      //                }
      //            }
      long endTime = System.currentTimeMillis();
      System.out.println(
          "Process by processRawHandle#invokeWithArguments time: " + (endTime - startTime) + "ms");

      startTime = System.currentTimeMillis();
      for (int j = 0; j < EPOCH; j++) {
        for (int i = 0; i < LOOP_COUNT; i++) {
          ColumnBuilder builder = TypeUtils.initColumnBuilder(TSDataType.INT64, ROW_COUNT);
          try {
            processMethodHandle2(columns.get(i), builder, processRawHandle);
          } catch (Throwable throwable) {
            throwable.printStackTrace();
          }

          if (i == 0) {
            System.out.println(builder.build().getLong(ROW_COUNT - 1));
          }
        }
      }
      endTime = System.currentTimeMillis();
      System.out.println(
          "Process by processRawHandle#invoke time: " + (endTime - startTime) + "ms");
    } catch (NoSuchMethodException | IllegalAccessException e) {
      e.printStackTrace();
    }

    // batch
    long startTime = System.currentTimeMillis();
    for (int j = 0; j < EPOCH; j++) {
      for (int i = 0; i < LOOP_COUNT; i++) {
        ColumnBuilder builder = TypeUtils.initColumnBuilder(TSDataType.INT64, ROW_COUNT);
        expScalarFunction.processBatch(columns.get(i), builder);
        if (i == 0) {
          System.out.println(builder.build().getLong(ROW_COUNT - 1));
        }
      }
    }
    long endTime = System.currentTimeMillis();
    System.out.println("Batch time: " + (endTime - startTime) + "ms");

    // single
    startTime = System.currentTimeMillis();
    for (int j = 0; j < EPOCH; j++) {
      for (int i = 0; i < LOOP_COUNT; i++) {
        ColumnBuilder builder = TypeUtils.initColumnBuilder(TSDataType.INT64, ROW_COUNT);
        expScalarFunction.processSingle(columns.get(i), builder);
        builder.build().getLong(0);
        if (i == 0) {
          System.out.println(builder.build().getLong(ROW_COUNT - 1));
        }
      }
    }
    endTime = System.currentTimeMillis();
    System.out.println("Single time: " + (endTime - startTime) + "ms");

    // row
    startTime = System.currentTimeMillis();
    for (int j = 0; j < EPOCH; j++) {
      for (int i = 0; i < LOOP_COUNT; i++) {
        ColumnBuilder builder = TypeUtils.initColumnBuilder(TSDataType.INT64, ROW_COUNT);
        try {
          expScalarFunction.processRow(columns.get(i), builder);
        } catch (IOException e) {
          e.printStackTrace();
        }
        if (i == 0) {
          System.out.println(builder.build().getLong(ROW_COUNT - 1));
        }
      }
    }
    endTime = System.currentTimeMillis();
    System.out.println("Row time: " + (endTime - startTime) + "ms");
  }
}
