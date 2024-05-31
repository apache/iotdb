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

import org.apache.iotdb.jdbc.IoTDBSQLException;
import org.apache.iotdb.jdbc.IoTDBStatement;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

public class JDBCCharsetExample {

  private static final Logger LOGGER = LoggerFactory.getLogger(JDBCCharsetExample.class);

  public static void main(String[] args) throws Exception {
    Class.forName("org.apache.iotdb.jdbc.IoTDBDriver");

    try (final Connection connection =
            DriverManager.getConnection(
                "jdbc:iotdb://127.0.0.1:6667?charset=GB18030", "root", "root");
        final IoTDBStatement statement = (IoTDBStatement) connection.createStatement()) {

      final String insertSQLWithGB18030 =
          "insert into root.测试(timestamp, 维语, 彝语, 繁体, 蒙文, 简体, 标点符号, 藏语) values(1, 'ئۇيغۇر تىلى', 'ꆈꌠꉙ', \"繁體\", 'ᠮᠣᠩᠭᠣᠯ ᠬᠡᠯᠡ', '简体', '——？！', \"བོད་སྐད།\");";
      final byte[] insertSQLWithGB18030Bytes = insertSQLWithGB18030.getBytes("GB18030");
      statement.execute(insertSQLWithGB18030Bytes);

      ResultSet resultSet = statement.executeQuery("select ** from root");
      outputResult(resultSet);
    } catch (IoTDBSQLException e) {
      LOGGER.error("IoTDB Jdbc example error", e);
    }
  }

  @SuppressWarnings({"squid:S106"})
  private static void outputResult(ResultSet resultSet)
      throws SQLException, UnsupportedEncodingException {
    if (resultSet != null) {
      System.out.println("--------------------------");
      final ResultSetMetaData metaData = resultSet.getMetaData();
      final int columnCount = metaData.getColumnCount();
      for (int i = 0; i < columnCount; i++) {
        System.out.print(metaData.getColumnLabel(i + 1) + " ");
      }
      System.out.println();

      while (resultSet.next()) {
        for (int i = 1; ; i++) {
          System.out.print(
              resultSet.getString(i) + " (" + new String(resultSet.getBytes(i), "GB18030") + ")");
          if (i < columnCount) {
            System.out.print(", ");
          } else {
            System.out.println();
            break;
          }
        }
      }
      System.out.println("--------------------------\n");
    }
  }
}
