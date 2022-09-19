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

import org.apache.iotdb.session.Session;
import org.apache.iotdb.session.util.Version;

import java.util.ArrayList;
import java.util.List;

public class Testt {
  public static void main(String[] args) throws Exception {
    insert();
    //    query();
  }

  private static void query() throws Exception {
    Session session =
        new Session.Builder()
            .host("127.0.0.1")
            .port(6667)
            .username("root")
            .password("root")
            .version(Version.V_0_13)
            .build();
    session.open(false);
    // set session fetchSize
    session.setFetchSize(10000);
    long time1 = System.currentTimeMillis();
    session.executeQueryStatement("select s1 from root.**;");
    long time2 = System.currentTimeMillis();
    System.out.println(time2 - time1);
    session.close();
  }

  private static void insert() throws Exception {
    Session session =
        new Session.Builder()
            .host("127.0.0.1")
            .port(6667)
            .username("root")
            .password("root")
            .version(Version.V_0_13)
            .build();
    session.open(false);
    // set session fetchSize
    session.setFetchSize(10000);
    List<String> measurements = new ArrayList<>();
    for (int j = 0; j < 400; j++) {
      measurements.add("s" + j);
    }
    for (int i = 0; i < 50; i++) {
      System.out.println(i);
      for (int k = 0; k < 100; k++) {
        List<String> values = new ArrayList<>();
        for (int j = 0; j < 400; j++) {
          values.add("" + k);
        }
        session.insertRecord("root.sg1.d" + i, k, measurements, values);
      }
    }
    session.executeNonQueryStatement("flush;");
    for (int i = 0; i < 50; i++) {
      System.out.println(i);
      for (int k = 100; k < 200; k++) {
        List<String> values = new ArrayList<>();
        for (int j = 0; j < 400; j++) {
          values.add("" + k);
        }
        session.insertRecord("root.sg1.d" + i, k, measurements, values);
      }
    }
    session.executeNonQueryStatement("flush;");
    for (int j = 0; j < 400; j++) {
      for (int k = 50; k < 150; k += 3) {
        String sql =
            String.format("delete from root.*.*.s%d where time>=%d and time<=%d;", j, k, (k + 2));
        session.executeNonQueryStatement(sql);
      }
    }
    session.close();
  }
}
