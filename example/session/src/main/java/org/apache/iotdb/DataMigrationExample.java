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
package org.apache.iotdb;

import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.SessionDataSet.DataIterator;
import org.apache.iotdb.session.pool.SessionDataSetWrapper;
import org.apache.iotdb.session.pool.SessionPool;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.write.record.Tablet;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * Migrate all data belongs to a path from one IoTDB to another IoTDB Each thread migrate one
 * series, the concurrent thread can be configured by concurrency
 *
 * <p>This example is migrating all timeseries from a local IoTDB with 6667 port to a local IoTDB
 * with 6668 port
 */
public class DataMigrationExample {

  // used to read data from the source IoTDB
  private static SessionPool readerPool;
  // used to write data into the destination IoTDB
  private static SessionPool writerPool;
  // concurrent thread of loading timeseries data
  private static int concurrency = 5;

  public static void main(String[] args)
      throws IoTDBConnectionException, StatementExecutionException, ExecutionException,
          InterruptedException {

    ExecutorService executorService = Executors.newFixedThreadPool(2 * concurrency + 1);

    String path = "root";

    if (args.length != 0) {
      path = args[0];
    }

    readerPool = new SessionPool("127.0.0.1", 6667, "root", "root", concurrency);
    writerPool = new SessionPool("127.0.0.1", 6668, "root", "root", concurrency);

    SessionDataSetWrapper schemaDataSet =
        readerPool.executeQueryStatement("count timeseries " + path);
    DataIterator schemaIter = schemaDataSet.iterator();
    int total;
    if (schemaIter.next()) {
      total = schemaIter.getInt(1);
      System.out.println("Total timeseries: " + total);
    } else {
      System.out.println("Can not get timeseries schema");
      System.exit(1);
    }
    readerPool.closeResultSet(schemaDataSet);

    schemaDataSet = readerPool.executeQueryStatement("show timeseries " + path);
    schemaIter = schemaDataSet.iterator();

    List<Future> futureList = new ArrayList<>();
    int count = 0;
    while (schemaIter.next()) {
      count++;
      Path currentPath = new Path(schemaIter.getString("timeseries"));
      Future future =
          executorService.submit(
              new LoadThread(
                  count, currentPath, TSDataType.valueOf(schemaIter.getString("dataType"))));
      futureList.add(future);
    }
    readerPool.closeResultSet(schemaDataSet);

    for (Future future : futureList) {
      future.get();
    }
    executorService.shutdown();

    readerPool.close();
    writerPool.close();
  }

  static class LoadThread implements Callable<Void> {

    String device;
    String measurement;
    Path series;
    TSDataType dataType;
    Tablet tablet;
    int i;

    public LoadThread(int i, Path series, TSDataType dataType) {
      this.i = i;
      this.device = series.getDevice();
      this.measurement = series.getMeasurement();
      this.dataType = dataType;
      this.series = series;
      List<IMeasurementSchema> schemaList = new ArrayList<>();
      schemaList.add(new MeasurementSchema(measurement, dataType));
      tablet = new Tablet(device, schemaList, 300000);
    }

    @Override
    public Void call() {

      SessionDataSetWrapper dataSet = null;

      try {

        dataSet =
            readerPool.executeQueryStatement(
                String.format("select %s from %s", measurement, device));

        DataIterator dataIter = dataSet.iterator();
        while (dataIter.next()) {
          int row = tablet.rowSize++;
          tablet.timestamps[row] = dataIter.getLong(1);
          switch (dataType) {
            case BOOLEAN:
              ((boolean[]) tablet.values[0])[row] = dataIter.getBoolean(2);
              break;
            case INT32:
              ((int[]) tablet.values[0])[row] = dataIter.getInt(2);
              break;
            case INT64:
              ((long[]) tablet.values[0])[row] = dataIter.getLong(2);
              break;
            case FLOAT:
              ((float[]) tablet.values[0])[row] = dataIter.getFloat(2);
              break;
            case DOUBLE:
              ((double[]) tablet.values[0])[row] = dataIter.getDouble(2);
              break;
            case TEXT:
              ((Binary[]) tablet.values[0])[row] = new Binary(dataIter.getString(2));
              break;
          }
          if (tablet.rowSize == tablet.getMaxRowNumber()) {
            writerPool.insertTablet(tablet, true);
            tablet.reset();
          }
        }
        if (tablet.rowSize != 0) {
          writerPool.insertTablet(tablet);
          tablet.reset();
        }

      } catch (Exception e) {
        System.out.println(
            "Loading the " + i + "-th timeseries: " + series + " failed " + e.getMessage());
        return null;
      } finally {
        readerPool.closeResultSet(dataSet);
      }

      System.out.println("Loading the " + i + "-th timeseries: " + series + " success");
      return null;
    }
  }
}
