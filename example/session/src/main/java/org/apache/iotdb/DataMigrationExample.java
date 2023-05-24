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

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.isession.SessionDataSet.DataIterator;
import org.apache.iotdb.isession.pool.SessionDataSetWrapper;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.pool.SessionPool;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.write.record.Tablet;
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
 * series, the concurrent thread can be configured by CONCURRENCY
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
  private static final int CONCURRENCY = 5;

  public static void main(String[] args)
      throws IoTDBConnectionException, StatementExecutionException, ExecutionException,
          InterruptedException, IllegalPathException {

    ExecutorService executorService = Executors.newFixedThreadPool(2 * CONCURRENCY + 1);

    String path = "root.**";

    if (args.length != 0) {
      path = args[0];
    }

    readerPool = new SessionPool("127.0.0.1", 6667, "root", "root", CONCURRENCY);
    writerPool = new SessionPool("127.0.0.1", 6668, "root", "root", CONCURRENCY);

    SessionDataSetWrapper deviceDataSet = readerPool.executeQueryStatement("count devices " + path);
    DataIterator deviceIter = deviceDataSet.iterator();
    int total;
    if (deviceIter.next()) {
      total = deviceIter.getInt(1);
      System.out.println("Total devices: " + total);
    } else {
      System.out.println("Can not get devices schema");
      System.exit(1);
    }
    readerPool.closeResultSet(deviceDataSet);

    deviceDataSet = readerPool.executeQueryStatement("show devices " + path);
    deviceIter = deviceDataSet.iterator();

    List<Future<Void>> futureList = new ArrayList<>();
    int count = 0;
    while (deviceIter.next()) {
      count++;
      Future<Void> future = executorService.submit(new LoadThread(count, deviceIter.getString("Device")));
      futureList.add(future);
    }
    readerPool.closeResultSet(deviceDataSet);

    for (Future<Void> future : futureList) {
      future.get();
    }
    executorService.shutdown();

    readerPool.close();
    writerPool.close();
  }

  static class LoadThread implements Callable<Void> {

    String device;
    Tablet tablet;
    int i;

    public LoadThread(int i, String device)
        throws IoTDBConnectionException, StatementExecutionException, IllegalPathException {
      this.i = i;
      this.device = device;
    }

    @Override
    public Void call() {
      SessionDataSetWrapper dataSet = null;

      try {
        dataSet = readerPool.executeQueryStatement(String.format("select * from %s", device));
        DataIterator dataIter = dataSet.iterator();
        List<String> columnNameList = dataIter.getColumnNameList();
        List<String> columnTypeList = dataIter.getColumnTypeList();
        List<MeasurementSchema> schemaList = new ArrayList<>();
        for (int j = 1; j < columnNameList.size(); j++) {
          PartialPath currentPath = new PartialPath(columnNameList.get(j));
          schemaList.add(
              new MeasurementSchema(
                  currentPath.getMeasurement(), TSDataType.valueOf(columnTypeList.get(j))));
        }
        tablet = new Tablet(device, schemaList, 300000);
        while (dataIter.next()) {
          int row = tablet.rowSize++;
          tablet.timestamps[row] = dataIter.getLong(1);
          for (int j = 0; j < schemaList.size(); ++j) {
            if (dataIter.isNull(j + 2)) {
              tablet.addValue(schemaList.get(j).getMeasurementId(), row, null);
              continue;
            }
            switch (schemaList.get(j).getType()) {
              case BOOLEAN:
                tablet.addValue(
                    schemaList.get(j).getMeasurementId(), row, dataIter.getBoolean(j + 2));
                break;
              case INT32:
                tablet.addValue(schemaList.get(j).getMeasurementId(), row, dataIter.getInt(j + 2));
                break;
              case INT64:
                tablet.addValue(schemaList.get(j).getMeasurementId(), row, dataIter.getLong(j + 2));
                break;
              case FLOAT:
                tablet.addValue(
                    schemaList.get(j).getMeasurementId(), row, dataIter.getFloat(j + 2));
                break;
              case DOUBLE:
                tablet.addValue(
                    schemaList.get(j).getMeasurementId(), row, dataIter.getDouble(j + 2));
                break;
              case TEXT:
                tablet.addValue(
                    schemaList.get(j).getMeasurementId(), row, dataIter.getString(j + 2));
                break;
              default:
                System.out.println("Migration of this type of data is not supported");
            }
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
            "Loading the " + i + "-th device: " + device + " failed " + e.getMessage());
        return null;
      } finally {
        if (dataSet != null) {
          readerPool.closeResultSet(dataSet);
        }
      }

      System.out.println("Loading the " + i + "-th device: " + device + " success");
      return null;
    }
  }
}
