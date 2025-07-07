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
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.TableSessionBuilder;

import org.apache.tsfile.enums.ColumnCategory;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.write.record.Tablet;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class ObjectExample {
  private static final String LOCAL_URL = "127.0.0.1:6667";

  public static void main(String[] args) {

    // don't specify database in constructor
    try (ITableSession session =
        new TableSessionBuilder()
            .nodeUrls(Collections.singletonList(LOCAL_URL))
            .username("root")
            .password("root")
            .build()) {
      session.executeNonQueryStatement("CREATE DATABASE test1");
      session.executeNonQueryStatement("use test1");

      // insert table data by tablet
      List<String> columnNameList =
          Arrays.asList("region_id", "plant_id", "device_id", "temperature", "file");
      List<TSDataType> dataTypeList =
          Arrays.asList(
              TSDataType.STRING,
              TSDataType.STRING,
              TSDataType.STRING,
              TSDataType.FLOAT,
              TSDataType.OBJECT);
      List<ColumnCategory> columnTypeList =
          new ArrayList<>(
              Arrays.asList(
                  ColumnCategory.TAG,
                  ColumnCategory.TAG,
                  ColumnCategory.TAG,
                  ColumnCategory.FIELD,
                  ColumnCategory.FIELD));
      Tablet tablet = new Tablet("test1", columnNameList, dataTypeList, columnTypeList, 1);
      int rowIndex = tablet.getRowSize();
      tablet.addTimestamp(rowIndex, 1);
      tablet.addValue(rowIndex, 0, "1");
      tablet.addValue(rowIndex, 1, "5");
      tablet.addValue(rowIndex, 2, "3");
      tablet.addValue(rowIndex, 3, 37.6F);
      tablet.addValue(
          rowIndex,
          4,
          true,
          0,
          Files.readAllBytes(
              Paths.get("/Users/ht/Downloads/2_1746622362350_fa24aa15233f4e76bcda789a5771f43f")));
      session.insert(tablet);
      tablet.reset();

      tablet = new Tablet("test1", columnNameList, dataTypeList, columnTypeList, 1);
      rowIndex = tablet.getRowSize();
      tablet.addTimestamp(rowIndex, 2);
      tablet.addValue(rowIndex, 0, "1");
      tablet.addValue(rowIndex, 1, "5");
      tablet.addValue(rowIndex, 2, "3");
      tablet.addValue(rowIndex, 3, 37.6F);
      tablet.addValue(
          rowIndex,
          4,
          true,
          0,
          Files.readAllBytes(
              Paths.get("/Users/ht/Downloads/2_1746622367063_8fb5ac8e21724140874195b60b878664")));
      session.insert(tablet);
      tablet.reset();

    } catch (IoTDBConnectionException e) {
      e.printStackTrace();
    } catch (StatementExecutionException e) {
      e.printStackTrace();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
