/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.session;

import org.apache.tsfile.encoding.table.ClusterTable;
import org.apache.tsfile.encoding.table.ClusterTableOptions;

import java.util.HashMap;
import java.util.Map;

/** Standalone local integration check. Arguments: host port dedicatedDevice write|read. */
public final class ClusterTableSessionIT {
  private ClusterTableSessionIT() {}

  public static void main(String[] args) throws Exception {
    if (args.length != 4 || (!args[3].equals("write") && !args[3].equals("read"))) {
      throw new IllegalArgumentException("host port dedicatedDevice write|read");
    }
    try (Session session =
        new Session.Builder()
            .host(args[0])
            .port(Integer.parseInt(args[1]))
            .username("root")
            .password("root")
            .enableRedirection(false)
            .build()) {
      session.open(false);
      for (ClusterTableOptions.Method method : ClusterTableOptions.Method.values()) {
        ClusterTableOptions options = new ClusterTableOptions(method, 8, 3, 42);
        ClusterTableSession tableSession =
            new ClusterTableSession(
                session,
                args[2] + (method == ClusterTableOptions.Method.ACLUSTER ? ".a" : ".k"),
                options);
        ClusterTable table = ClusterTableSessionTest.sample(1200);
        if (args[3].equals("write")) {
          tableSession.createSchema();
          tableSession.writeBlock(1, table.slice(0, 600));
          tableSession.writeBlock(2, table.slice(600, 1200));
          session.executeNonQueryStatement("flush");
        }
        Map<String, Integer> expected = new HashMap<>(), actual = new HashMap<>();
        ClusterTableSessionTest.records(expected, table);
        tableSession.readAll(page -> ClusterTableSessionTest.records(actual, page));
        if (!expected.equals(actual))
          throw new AssertionError("Complete-record mismatch: " + method);
        expected.clear();
        actual.clear();
        long start = 1700000000010L, end = 1700000000020L;
        ClusterTableSessionTest.records(expected, table.timeRange(start, end));
        tableSession.readTimeRange(
            start, end, page -> ClusterTableSessionTest.records(actual, page));
        if (!expected.equals(actual))
          throw new AssertionError("Original-time query mismatch: " + method);
        System.out.println(
            method + ": 1200 exact records; original-time range passed; mode=" + args[3]);
      }
    }
  }
}
