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
package org.apache.iotdb.doublewrite;

import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.pool.SessionDataSetWrapper;

/**
 * This is a simple double write example java class, which shows how to enable double write feature
 * and proves that the double write feature performs correctly. To run this code, you need to start
 * another IoTDB service. The easiest way to do this is to copy the
 * ./server/target/iotdb-server-0.12.5-SNAPSHOT folder to any location. Then change the default port
 * in iotdb-engine.properties to 6668, and start the service using the start-server script. Or you
 * can start IoTDB-B on another computer and modify the configuration of IoTDB-B in DoubleWriteUtil.
 * Finally, you can run this code and see double write feature from the command line.
 */
public class DoubleWriteExample extends DoubleWriteUtil {

  private static final String dA = "d0";
  private static final String dB = "d0";

  private static final int batchCnt = 10;
  private static final int timeseriesCnt = 1;
  private static final int batchSize = 1;

  private static final String sql = "select * from root.DOUBLEWRITESG.d0";

  public static void main(String[] args) throws Exception {
    initEnvironment();
    initSessionPool(dA, dB, batchCnt, timeseriesCnt, batchSize);
    insertData();
    queryData();
    cleanEnvironment();
  }

  private static void insertData() throws InterruptedException {
    threadA.start();
    threadB.start();

    threadA.join();
    threadB.join();
  }

  private static void queryData() throws IoTDBConnectionException, StatementExecutionException {
    System.out.println("Data in IoTDB-A:");
    // select data from IoTDB-A
    SessionDataSetWrapper wrapper = sessionPoolA.executeQueryStatement(sql);
    while (wrapper.hasNext()) {
      System.out.println(wrapper.next());
    }

    // select data from IoTDB-B
    System.out.println("Data in IoTDB-B:");
    wrapper = sessionPoolB.executeQueryStatement(sql);
    while (wrapper.hasNext()) {
      System.out.println(wrapper.next());
    }
  }
}
