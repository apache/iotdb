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

import org.apache.iotdb.isession.ITableSession;
import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.isession.pool.ITableSessionPool;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.pool.TableSessionPoolBuilder;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.record.Tablet.ColumnCategory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class TableModelSessionPoolExample {

  private static final String LOCAL_URL = "127.0.0.1:6667";

  public static void main(String[] args) {

    // don't specify database in constructor
    ITableSessionPool tableSessionPool =
        new TableSessionPoolBuilder()
            .nodeUrls(Collections.singletonList(LOCAL_URL))
            .user("root")
            .password("root")
            .maxSize(1)
            .build();

    try (ITableSession session = tableSessionPool.getSession()) {

      session.executeNonQueryStatement("CREATE DATABASE test1");
      session.executeNonQueryStatement("CREATE DATABASE test2");

      session.executeNonQueryStatement("use test2");

      // or use full qualified table name
      session.executeNonQueryStatement(
          "create table test1.table1("
              + "region_id STRING ID, "
              + "plant_id STRING ID, "
              + "device_id STRING ID, "
              + "model STRING ATTRIBUTE, "
              + "temperature FLOAT MEASUREMENT, "
              + "humidity DOUBLE MEASUREMENT) with (TTL=3600000)");

      session.executeNonQueryStatement(
          "create table table2("
              + "region_id STRING ID, "
              + "plant_id STRING ID, "
              + "color STRING ATTRIBUTE, "
              + "temperature FLOAT MEASUREMENT, "
              + "speed DOUBLE MEASUREMENT) with (TTL=6600000)");

      // show tables from current database
      try (SessionDataSet dataSet = session.executeQueryStatement("SHOW TABLES")) {
        System.out.println(dataSet.getColumnNames());
        System.out.println(dataSet.getColumnTypes());
        while (dataSet.hasNext()) {
          System.out.println(dataSet.next());
        }
      }

      // show tables by specifying another database
      // using SHOW tables FROM
      try (SessionDataSet dataSet = session.executeQueryStatement("SHOW TABLES FROM test1")) {
        System.out.println(dataSet.getColumnNames());
        System.out.println(dataSet.getColumnTypes());
        while (dataSet.hasNext()) {
          System.out.println(dataSet.next());
        }
      }

      // insert table data by tablet
      List<String> measurementNameList =
          Arrays.asList("region_id", "plant_id", "device_id", "model", "temperature", "humidity");
      List<TSDataType> dataTypeList =
          Arrays.asList(
              TSDataType.STRING,
              TSDataType.STRING,
              TSDataType.STRING,
              TSDataType.STRING,
              TSDataType.FLOAT,
              TSDataType.DOUBLE);
      List<ColumnCategory> columnTypeList =
          new ArrayList<>(
              Arrays.asList(
                  ColumnCategory.ID,
                  ColumnCategory.ID,
                  ColumnCategory.ID,
                  ColumnCategory.ATTRIBUTE,
                  ColumnCategory.MEASUREMENT,
                  ColumnCategory.MEASUREMENT));
      Tablet tablet = new Tablet("test1", measurementNameList, dataTypeList, columnTypeList, 100);
      for (long timestamp = 0; timestamp < 100; timestamp++) {
        int rowIndex = tablet.getRowSize();
        tablet.addTimestamp(rowIndex, timestamp);
        tablet.addValue("region_id", rowIndex, "1");
        tablet.addValue("plant_id", rowIndex, "5");
        tablet.addValue("device_id", rowIndex, "3");
        tablet.addValue("model", rowIndex, "A");
        tablet.addValue("temperature", rowIndex, 37.6F);
        tablet.addValue("humidity", rowIndex, 111.1);
        if (tablet.getRowSize() == tablet.getMaxRowNumber()) {
          session.insert(tablet);
          tablet.reset();
        }
      }
      if (tablet.getRowSize() != 0) {
        session.insert(tablet);
        tablet.reset();
      }

      // query table data
      try (SessionDataSet dataSet =
          session.executeQueryStatement(
              "select * from test1 "
                  + "where region_id = '1' and plant_id in ('3', '5') and device_id = '3'")) {
        System.out.println(dataSet.getColumnNames());
        System.out.println(dataSet.getColumnTypes());
        while (dataSet.hasNext()) {
          System.out.println(dataSet.next());
        }
      }

    } catch (IoTDBConnectionException e) {
      e.printStackTrace();
    } catch (StatementExecutionException e) {
      e.printStackTrace();
    } finally {
      tableSessionPool.close();
    }

    // specify database in constructor
    tableSessionPool =
        new TableSessionPoolBuilder()
            .nodeUrls(Collections.singletonList(LOCAL_URL))
            .user("root")
            .password("root")
            .maxSize(1)
            .database("test1")
            .build();

    try (ITableSession session = tableSessionPool.getSession()) {

      // show tables from current database
      try (SessionDataSet dataSet = session.executeQueryStatement("SHOW TABLES")) {
        System.out.println(dataSet.getColumnNames());
        System.out.println(dataSet.getColumnTypes());
        while (dataSet.hasNext()) {
          System.out.println(dataSet.next());
        }
      }

      // change database to test2
      session.executeNonQueryStatement("use test2");

      // show tables by specifying another database
      // using SHOW tables FROM
      try (SessionDataSet dataSet = session.executeQueryStatement("SHOW TABLES")) {
        System.out.println(dataSet.getColumnNames());
        System.out.println(dataSet.getColumnTypes());
        while (dataSet.hasNext()) {
          System.out.println(dataSet.next());
        }
      }

    } catch (IoTDBConnectionException e) {
      e.printStackTrace();
    } catch (StatementExecutionException e) {
      e.printStackTrace();
    }

    try (ITableSession session = tableSessionPool.getSession()) {

      // show tables from default database test1
      try (SessionDataSet dataSet = session.executeQueryStatement("SHOW TABLES")) {
        System.out.println(dataSet.getColumnNames());
        System.out.println(dataSet.getColumnTypes());
        while (dataSet.hasNext()) {
          System.out.println(dataSet.next());
        }
      }

    } catch (IoTDBConnectionException e) {
      e.printStackTrace();
    } catch (StatementExecutionException e) {
      e.printStackTrace();
    } finally {
      tableSessionPool.close();
    }
  }
}
