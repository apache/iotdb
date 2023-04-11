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

import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.isession.util.Version;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;

public class QueryTest {

  private static Session session;
  private static Session sessionEnableRedirect;

  private static final String HOST = "192.168.130.9";

  public static void main(String[] args)
      throws IoTDBConnectionException, StatementExecutionException {
    session =
        new Session.Builder()
            .host(HOST)
            .port(6667)
            .username("root")
            .password("root")
            .version(Version.V_1_0)
            .build();
    session.open(false);

    // set session fetchSize
    session.setFetchSize(10000);
    long startTime = System.currentTimeMillis();
    String querySql =
        "select * from root.** where time >= 2022-01-01T00:00:01.000+08:00 and time <= 2022-01-01T00:01:00.000+08:00 align by device";
    try (SessionDataSet dataSet = session.executeQueryStatement(querySql)) {
      while (dataSet.hasNext()) {
        dataSet.next();
      }
    }
    System.out.printf("Cost: %d", System.currentTimeMillis() - startTime);
  }
}
