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
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.TableSessionBuilder;

import org.apache.tsfile.read.common.RowRecord;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class TableModelSessionExample2 {

  private static final String LOCAL_URL = "127.0.0.1:6667";

  public static void main(String[] args) {

    Map<String, Integer> map = new HashMap<>();

    // don't specify database in constructor
    try (ITableSession session =
        new TableSessionBuilder()
            .nodeUrls(Collections.singletonList(LOCAL_URL))
            .username("root")
            .password("root")
            .build()) {

      session.executeNonQueryStatement("use test_g_0");

      // show tables from current database
      try (SessionDataSet dataSet =
          session.executeQueryStatement("SELECT time, device_id, s_0 from table_0'")) {
        while (dataSet.hasNext()) {
          RowRecord record = dataSet.next();
          String device = record.getField(1).getStringValue();
          if (map.containsKey(device)) {
            map.put(device, map.get(device) + 1);
          } else {
            map.put(device, 1);
            System.out.println(device + " " + map.size());
          }
        }
      }
      for (Map.Entry<String, Integer> entry : map.entrySet()) {
        System.out.println(entry.getKey() + " : " + entry.getValue());
      }

    } catch (IoTDBConnectionException e) {
      e.printStackTrace();
    } catch (StatementExecutionException e) {
      e.printStackTrace();
    }
  }
}
