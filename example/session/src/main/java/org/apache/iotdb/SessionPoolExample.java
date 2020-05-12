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

import java.util.Collections;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.pool.SessionDataSetWrapper;
import org.apache.iotdb.session.pool.SessionPool;

public class SessionPoolExample {

  private static SessionPool pool;
  private static ExecutorService service;

  public static void main(String[] args)
      throws StatementExecutionException, IoTDBConnectionException {
    pool = new SessionPool("127.0.0.1", 6667, "root", "root", 3);

    insertRecord();
    queryGettingAllData();
    queryGettingPartData();
    deleteData();
    pool.close();
  }

  // more insert example, see SessionExample.java
  private static void insertRecord() throws StatementExecutionException, IoTDBConnectionException {
    for (int i = 0; i < 10; i++) {
      pool.insertRecord("root.sg1.d1", i, Collections.singletonList("s" + i),
          Collections.singletonList("" + i));
    }
  }

  // Query getting all data
  private static void queryGettingAllData() {
    service = Executors.newFixedThreadPool(10);
    for (int i = 0; i < 10; i++) {
      final int no = i;
      service.submit(() -> {
        SessionDataSetWrapper wrapper = null;
        try {
          wrapper = pool
              .executeQueryStatement("select * from root.sg1.d1 where time = " + no);
          // if you get all data, don't calling closeResultSet() is OK.
          // but it's suggested.
          while (wrapper.hasNext()) {
            System.out.println(wrapper.next());
          }
        } catch (IoTDBConnectionException | StatementExecutionException e) {
          // if there is exception when you call SessionDataSetWrapper.hasNext() or next(),
          // you have to call closeResultSet().
          try {
            pool.closeResultSet(wrapper);
          } catch (StatementExecutionException ex) {
            ex.printStackTrace();
          }
          e.printStackTrace();
        }
      });
    }
    service.shutdown();
  }

  // Query getting part data
  private static void queryGettingPartData() {
    service = Executors.newFixedThreadPool(10);
    for (int i = 0; i < 10; i++) {
      final int no = i;
      service.submit(() -> {
        try {
          SessionDataSetWrapper wrapper = pool
              .executeQueryStatement("select * from root.sg1.d1 where time = " + no);
          wrapper.next();
          // REMEMBER to call closeResultSet() here.
          // otherwise the query left will be blocked.
          pool.closeResultSet(wrapper);
        } catch (IoTDBConnectionException | StatementExecutionException e) {
          e.printStackTrace();
        }
      });
    }
    service.shutdown();
  }

  private static void deleteData() throws IoTDBConnectionException, StatementExecutionException {
    String path = "root.sg1.d1";
    long deleteTime = 99;
    pool.deleteData(path, deleteTime);
  }

}
