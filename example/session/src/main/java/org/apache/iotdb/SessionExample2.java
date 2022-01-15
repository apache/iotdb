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
import org.apache.iotdb.session.Session;
import org.apache.iotdb.session.SessionDataSet;

@SuppressWarnings("squid:S106")
public class SessionExample2 {

  private static Session session;
  private static final String LOCAL_HOST = "127.0.0.1";

  static String statement1 =
      "select min_time(s1), first_value(s1), max_time(s1), last_value(s1), "
          + "min_value(s1), max_value(s1) from root.ln.ordered";

  static String statement2 =
      "select min_time(s1), first_value(s1), max_time(s1), last_value(s1), min_value(s1), max_value(s1) from root.ln.ordered";

  static String statement3 =
      "select min_time(s1), first_value(s1), max_time(s1), last_value(s1), min_value(s1), max_value(s1) from root.ln.ordered group by ([0, 10000), 1000ms)";

  public static void main(String[] args)
      throws IoTDBConnectionException, StatementExecutionException {
    session = new Session(LOCAL_HOST, 6667, "root", "root");
    session.open(false);

    // set session fetchSize
    session.setFetchSize(10000);

    SessionDataSet dataSet = session.executeQueryStatement(statement3);

    System.out.println(dataSet.getColumnNames());
    dataSet.setFetchSize(1024); // default is 10000
    while (dataSet.hasNext()) {
      System.out.println(dataSet.next());
    }
    dataSet.closeOperationHandle();

    session.close();
  }
}
