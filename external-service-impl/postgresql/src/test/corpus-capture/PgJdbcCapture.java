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

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Properties;

/** Records what pgJDBC (DBeaver's driver) sends: metadata, prepared statements, fetch size. */
public class PgJdbcCapture {
  public static void main(String[] args) throws Exception {
    Properties props = new Properties();
    props.setProperty("user", "postgres");
    props.setProperty("password", Files.readString(Path.of(args[0])).trim());
    props.setProperty("ApplicationName", "pgjdbc");
    String url = "jdbc:postgresql://127.0.0.1:54314/iotdb";
    try (Connection c = DriverManager.getConnection(url, props)) {
      DatabaseMetaData md = c.getMetaData();
      drain(md.getCatalogs());
      drain(md.getSchemas());
      drain(md.getTables(null, "refdb", "%", new String[] {"TABLE", "VIEW"}));
      drain(md.getColumns(null, "refdb", "sensors", "%"));
      drain(md.getPrimaryKeys(null, "refdb", "sensors"));
      drain(md.getIndexInfo(null, "refdb", "sensors", false, true));
      drain(md.getImportedKeys(null, "refdb", "sensors"));
      drain(md.getTypeInfo());
      try (Statement s = c.createStatement()) {
        drain(s.executeQuery("SELECT * FROM refdb.sensors LIMIT 5"));
      }
      try (PreparedStatement p =
          c.prepareStatement(
              "SELECT \"time\", machine, temperature FROM refdb.sensors"
                  + " WHERE machine = ? AND \"time\" >= ? ORDER BY \"time\" LIMIT ?")) {
        for (int i = 1; i <= 6; i++) { // the 5th execution switches to a server-side prepared statement
          p.setString(1, "m" + (i % 5 + 1));
          p.setTimestamp(2, Timestamp.from(Instant.parse("2026-10-25T00:00:00Z")));
          p.setInt(3, 10);
          drain(p.executeQuery());
        }
      }
      c.setAutoCommit(false);
      try (Statement s = c.createStatement()) {
        s.setFetchSize(1000); // portal with row limit
        drain(s.executeQuery("SELECT * FROM refdb.events"));
      }
      c.commit();
    }
  }

  private static void drain(ResultSet rs) throws Exception {
    try (rs) {
      int columns = rs.getMetaData().getColumnCount();
      while (rs.next()) {
        for (int i = 1; i <= columns; i++) {
          rs.getObject(i);
        }
      }
    }
  }
}
