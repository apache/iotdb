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
  public static void main(String[] args)
      throws IoTDBConnectionException, StatementExecutionException, ExecutionException,
          InterruptedException {

    long start = System.currentTimeMillis();
    String path = "root.**";
    args = new String[] {"127.0.0.1", "6667", "root", "127.0.0.1", "6668", "root", "1"};
    ExecutorService executorService = null;
    if (args == null || args.length != 7) {
      System.out.println(
          "Correct input parameters are required. For example, java -jar xxx.jar sourceIP sourcePort sourcePassword destIP destPort destPassword concurrency(ThreadNumber = 2*concurrency+1)");
      System.exit(1);
    }
    try {
      int concurrency = Integer.parseInt(args[6]);
      executorService = Executors.newFixedThreadPool(2 * concurrency + 1);
      readerPool =
          new SessionPool(args[0], Integer.parseInt(args[1]), "root", args[2], 2 * concurrency + 1);
      writerPool =
          new SessionPool(args[3], Integer.parseInt(args[4]), "root", args[5], 2 * concurrency + 1);
    } catch (Exception e) {
      System.out.println(
          "Correct input parameters are required. For example, java -jar xxx.jar sourceIP sourcePort sourcePassword destIP destPort destPassword concurrency(ThreadNumber = 2*concurrency+1)");
      System.exit(1);
    }

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

    List<Future> futureList = new ArrayList<>();
    int count = 0;
    while (deviceIter.next()) {
      count++;
      Future future = executorService.submit(new LoadThread(count, deviceIter.getString("Device")));
      futureList.add(future);
    }
    readerPool.closeResultSet(deviceDataSet);

    for (Future future : futureList) {
      future.get();
    }
    System.out.println("Total cost: " + (System.currentTimeMillis() - start) + "ms");
    executorService.shutdown();

    readerPool.close();
    writerPool.close();
  }

  static class LoadThread implements Callable<Void> {

    String device;
    Tablet tablet;
    int i;

    public LoadThread(int i, String device) {
      this.i = i;
      this.device = device;
    }

    @Override
    public Void call() {
      SessionDataSetWrapper dataSet = null;
      long startTime = System.nanoTime();
      try {
        dataSet = readerPool.executeQueryStatement(String.format("select * from %s", device));
        DataIterator dataIter = dataSet.iterator();
        List<String> columnNameList = dataIter.getColumnNameList();
        List<String> columnTypeList = dataIter.getColumnTypeList();
        List<MeasurementSchema> schemaList = new ArrayList<>();
        List<Integer> measureMentValueList = new ArrayList<>();
        for (int j = 1; j < columnNameList.size(); j++) {
          String measurement =
              columnNameList.get(j).substring(columnNameList.get(j).lastIndexOf(".") + 1);
          if (measurement.contains("`")) {
            continue;
          }
          schemaList.add(
              new MeasurementSchema(measurement, TSDataType.valueOf(columnTypeList.get(j))));
          measureMentValueList.add(j + 1);
        }
        if (schemaList.isEmpty()) {
          return null;
        }
        tablet = new Tablet(device, schemaList, 300000);
        while (dataIter.next()) {
          int row = tablet.rowSize++;
          tablet.timestamps[row] = dataIter.getLong(1);
          for (int j = 0; j < schemaList.size(); ++j) {
            if (dataIter.isNull(measureMentValueList.get(j))) {
              tablet.addValue(schemaList.get(j).getMeasurementId(), row, null);
              continue;
            }
            switch (schemaList.get(j).getType()) {
              case BOOLEAN:
                tablet.addValue(
                    schemaList.get(j).getMeasurementId(),
                    row,
                    dataIter.getBoolean(measureMentValueList.get(j)));
                break;
              case INT32:
                tablet.addValue(
                    schemaList.get(j).getMeasurementId(),
                    row,
                    dataIter.getInt(measureMentValueList.get(j)));
                break;
              case INT64:
                tablet.addValue(
                    schemaList.get(j).getMeasurementId(),
                    row,
                    dataIter.getLong(measureMentValueList.get(j)));
                break;
              case FLOAT:
                tablet.addValue(
                    schemaList.get(j).getMeasurementId(),
                    row,
                    dataIter.getFloat(measureMentValueList.get(j)));
                break;
              case DOUBLE:
                tablet.addValue(
                    schemaList.get(j).getMeasurementId(),
                    row,
                    dataIter.getDouble(measureMentValueList.get(j)));
                break;
              case TEXT:
                tablet.addValue(
                    schemaList.get(j).getMeasurementId(),
                    row,
                    dataIter.getString(measureMentValueList.get(j)));
                break;
              default:
                System.out.println("Migration of this type of data is not supported");
            }
          }
          if (tablet.rowSize == tablet.getMaxRowNumber()) {
            writerPool.insertTablet(tablet, true);
            System.out.println("device: " + device + " migrates " + 300000 + " data");
            tablet.reset();
          }
        }
        if (tablet.rowSize != 0) {
          writerPool.insertTablet(tablet);
          System.out.println("device: " + device + " migrates " + tablet.rowSize + " data");
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
        long endTime = System.nanoTime();
        long totalTime = endTime - startTime;
        System.out.println("device ：" + device + " 运行耗时 " + totalTime + " ms");
      }

      System.out.println("Loading the " + i + "-th device: " + device + " success");
      return null;
    }
  }
}
