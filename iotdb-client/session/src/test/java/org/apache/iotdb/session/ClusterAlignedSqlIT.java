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

import org.apache.iotdb.isession.SessionDataSet;

import org.apache.tsfile.read.common.RowRecord;

/** Live native SQL check against an isolated server. Arguments: host port database write|read. */
public final class ClusterAlignedSqlIT {
  private static final int ROWS = 1200;
  private static final long START = 1700000000000L;

  private ClusterAlignedSqlIT() {}

  public static void main(String[] args) throws Exception {
    if (args.length != 4
        || !args[2].matches("root\\.[A-Za-z_][A-Za-z0-9_]*")
        || (!args[3].equals("write") && !args[3].equals("read"))) {
      throw new IllegalArgumentException("host port dedicatedDatabase write|read");
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
      if (args[3].equals("write")) {
        for (String method : new String[] {"ACLUSTER", "KCLUSTER"}) {
          String device = args[2] + "." + method.toLowerCase(java.util.Locale.ROOT);
          session.executeNonQueryStatement(
              "CREATE ALIGNED TIMESERIES "
                  + device
                  + " ("
                  + "a DOUBLE ENCODING="
                  + method
                  + ", b INT32 ENCODING="
                  + method
                  + ", c DOUBLE ENCODING="
                  + method
                  + ", d INT64 ENCODING="
                  + method
                  + ", empty INT32 ENCODING="
                  + method
                  + ")");
          insert(session, device, 0, 1000);
          session.executeNonQueryStatement("FLUSH");
          verify(session, device, 1000);
        }
        for (String method : new String[] {"acluster", "kcluster"}) {
          String device = args[2] + ".scalar_" + method;
          session.executeNonQueryStatement(
              "CREATE TIMESERIES " + device + ".v WITH DATATYPE=FLOAT ENCODING=" + method);
          for (int start = 0; start < 400; start += 50) {
            StringBuilder sql =
                new StringBuilder("INSERT INTO ").append(device).append("(time,v) VALUES ");
            for (int r = start; r < start + 50; r++) {
              if (r != start) sql.append(',');
              sql.append('(').append(START + r).append(',').append((float) c(r)).append(')');
            }
            session.executeNonQueryStatement(sql.toString());
          }
        }
        session.executeNonQueryStatement("FLUSH");
        // Acknowledge ordinary inserts without FLUSH, then test WAL recovery after process death.
        for (String method : new String[] {"acluster", "kcluster"})
          insert(session, args[2] + "." + method, 1000, ROWS);
      }
      for (String method : new String[] {"acluster", "kcluster"}) {
        verify(session, args[2] + "." + method, ROWS);
        try (SessionDataSet scalar =
            session.executeQueryStatement("SELECT v FROM " + args[2] + ".scalar_" + method)) {
          int r = 0;
          while (scalar.hasNext()) {
            RowRecord row = scalar.next();
            require(row.getTimestamp() == START + r, "scalar timestamp", r);
            require(
                Float.floatToRawIntBits(row.getFields().get(0).getFloatV())
                    == Float.floatToRawIntBits((float) c(r)),
                "scalar value",
                r++);
          }
          require(r == 400, "scalar count", r);
        }
        System.out.println(
            method
                + ": "
                + ROWS
                + " aligned SQL rows plus 400 scalar SQL rows, projection, value/time predicates, descending scan, aggregates passed; mode="
                + args[3]);
      }
    }
  }

  private static double a(int r) {
    return ((r * 17) % 37) / 10.0;
  }

  private static int b(int r) {
    return ((r * 17) % 37) * 2;
  }

  private static double c(int r) {
    return r % 13 == 0 ? -0.0 : ((r * 3) % 19) / 10.0;
  }

  private static long d(int r) {
    return r % 11 == 0 ? Long.MAX_VALUE - r : 9007199254740993L + r;
  }

  private static void insert(Session session, String device, int from, int to) throws Exception {
    for (int start = from; start < to; start += 50) {
      StringBuilder sql =
          new StringBuilder("INSERT INTO ").append(device).append("(time,a,b,c,d) ALIGNED VALUES ");
      for (int r = start; r < Math.min(start + 50, to); r++) {
        if (r != start) sql.append(',');
        sql.append('(')
            .append(START + r)
            .append(',')
            .append(a(r))
            .append(',')
            .append(b(r))
            .append(',')
            .append(r % 7 == 0 ? "null" : Double.toString(c(r)))
            .append(',')
            .append(d(r))
            .append(')');
      }
      session.executeNonQueryStatement(sql.toString());
    }
  }

  private static void verify(Session session, String device, int rows) throws Exception {
    try (SessionDataSet data = session.executeQueryStatement("SELECT a,b,c,d FROM " + device)) {
      int r = 0;
      while (data.hasNext()) {
        RowRecord row = data.next();
        require(row.getTimestamp() == START + r, "timestamp", r);
        require(
            Double.doubleToRawLongBits(row.getFields().get(0).getDoubleV())
                == Double.doubleToRawLongBits(a(r)),
            "a",
            r);
        require(row.getFields().get(1).getIntV() == b(r), "b", r);
        if (r % 7 == 0) require(row.getFields().get(2).getDataType() == null, "null", r);
        else
          require(
              Double.doubleToRawLongBits(row.getFields().get(2).getDoubleV())
                  == Double.doubleToRawLongBits(c(r)),
              "c",
              r);
        require(row.getFields().get(3).getLongV() == d(r), "d", r);
        r++;
      }
      require(r == rows, "row count", r);
    }
    try (SessionDataSet data = session.executeQueryStatement("SELECT b FROM " + device)) {
      int r = 0;
      while (data.hasNext()) {
        RowRecord row = data.next();
        require(
            row.getTimestamp() == START + r && row.getFields().get(0).getIntV() == b(r),
            "projection",
            r++);
      }
      require(r == rows, "projection count", r);
    }
    try (SessionDataSet data =
        session.executeQueryStatement(
            "SELECT a,d FROM "
                + device
                + " WHERE time >= "
                + (START + 125)
                + " AND time < "
                + (START + 377)
                + " AND b >= 40 ORDER BY TIME DESC")) {
      int r = 376, count = 0;
      while (data.hasNext()) {
        while (r >= 125 && b(r) < 40) r--;
        RowRecord row = data.next();
        require(r >= 125 && row.getTimestamp() == START + r, "filtered time", r);
        require(
            row.getFields().get(0).getDoubleV() == a(r)
                && row.getFields().get(1).getLongV() == d(r),
            "filtered value",
            r);
        r--;
        count++;
      }
      int expected = 0;
      for (int i = 125; i < 377; i++) if (b(i) >= 40) expected++;
      require(count == expected, "predicate count", count);
    }
    try (SessionDataSet data =
        session.executeQueryStatement(
            "SELECT count(a),count(c),sum(b),first_value(b),last_value(b) FROM " + device)) {
      require(data.hasNext(), "aggregate row", 0);
      RowRecord row = data.next();
      long sum = 0;
      for (int r = 0; r < rows; r++) sum += b(r);
      require(row.getFields().get(0).getLongV() == rows, "count(a)", 0);
      require(row.getFields().get(1).getLongV() == rows - (rows + 6) / 7, "count(c)", 0);
      require(row.getFields().get(2).getDoubleV() == sum, "sum(b)", 0);
      require(row.getFields().get(3).getIntV() == b(0), "first_value", 0);
      require(row.getFields().get(4).getIntV() == b(rows - 1), "last_value", 0);
    }
  }

  private static void require(boolean condition, String what, int row) {
    if (!condition) throw new AssertionError(what + " at row " + row);
  }
}
