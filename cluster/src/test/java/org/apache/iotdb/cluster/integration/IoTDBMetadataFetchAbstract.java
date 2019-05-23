/**
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
package org.apache.iotdb.cluster.integration;

import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import org.apache.iotdb.jdbc.Config;
import org.apache.iotdb.jdbc.Constant;
import org.junit.Assert;

public abstract class IoTDBMetadataFetchAbstract {

  protected void test(String url, boolean isBatch) throws SQLException {
    try (Connection connection = DriverManager
        .getConnection(Config.IOTDB_URL_PREFIX + url, "root", "root")) {
      insertSQL(connection, isBatch);
      testShowStorageGroup(connection);
      testDatabaseMetadata(connection);
      testShowTimeseries(connection);
      testShowTimeseriesPath(connection);
    }
  }


  protected void insertSQL(Connection connection, boolean isBatch) throws SQLException {
    Statement statement = connection.createStatement();
    String[] insertSqls = new String[]{
        "SET STORAGE GROUP TO root.ln.wf01",
        "SET STORAGE GROUP TO root.ln.wf02",
        "SET STORAGE GROUP TO root.ln.wf03",
        "SET STORAGE GROUP TO root.ln.wf04",
        "SET STORAGE GROUP TO root.ln.wf05",
        "CREATE TIMESERIES root.ln.wf01.wt01.status WITH DATATYPE = BOOLEAN, ENCODING = PLAIN",
        "CREATE TIMESERIES root.ln.wf01.wt01.temperature WITH DATATYPE = FLOAT, ENCODING = RLE, compressor = SNAPPY, MAX_POINT_NUMBER = 3",
        "CREATE TIMESERIES root.ln.wf01.wt02.humidity WITH DATATYPE = DOUBLE, ENCODING = RLE",

        "CREATE TIMESERIES root.ln.wf02.wt03.status WITH DATATYPE = INT32, ENCODING = PLAIN",
        "CREATE TIMESERIES root.ln.wf02.wt04.temperature WITH DATATYPE = FLOAT, ENCODING = RLE",

        "CREATE TIMESERIES root.ln.wf03.wt02.status WITH DATATYPE = INT64, ENCODING = PLAIN",
        "CREATE TIMESERIES root.ln.wf03.wt03.temperature WITH DATATYPE = FLOAT, ENCODING = TS_2DIFF, MAX_POINT_NUMBER = 5",

        "CREATE TIMESERIES root.ln.wf04.wt04.status WITH DATATYPE = TEXT, ENCODING = PLAIN",
        "CREATE TIMESERIES root.ln.wf04.wt05.temperature WITH DATATYPE = FLOAT, ENCODING = GORILLA",

        "CREATE TIMESERIES root.ln.wf05.wt01.status WITH DATATYPE = DOUBLE, ENCODING = PLAIN",
    };
    if (isBatch) {
      for (String sql : insertSqls) {
        statement.addBatch(sql);
      }
      statement.executeBatch();
      statement.clearBatch();
    } else {
      for (String sql : insertSqls) {
        statement.execute(sql);
      }
    }
    statement.close();
  }

  protected void testShowStorageGroup(Connection connection) throws SQLException {
    Statement statement = connection.createStatement();
    String[] sqls = new String[]{
        "show storage group",

    };
    String[] standards = new String[]{
        "root.ln.wf04,\n"
            + "root.ln.wf03,\n"
            + "root.ln.wf02,\n"
            + "root.ln.wf01,\n"
            + "root.ln.wf05,\n",
    };
    checkCorrectness(sqls, standards, statement);
  }

  protected void testShowTimeseriesPath(Connection connection) throws SQLException {
    Statement statement = connection.createStatement();
    String[] sqls = new String[]{
        "show timeseries root.ln.wf01.wt01.status", // full seriesPath
        "show timeseries root.ln", // prefix seriesPath
        "show timeseries root.ln.wf01.wt01", // prefix seriesPath
        "show timeseries root.ln.*.wt01.status", // seriesPath with stars
        "show timeseries root.ln.*.wt01.*", // seriesPath with stars
        "show timeseries root.a.b", // nonexistent timeseries, thus returning ""
        "show timeseries root.ln,root.ln",
        // SHOW TIMESERIES <PATH> only accept single seriesPath, thus
        // returning ""
    };
    String[] standards = new String[]{
        "root.ln.wf01.wt01.status,root.ln.wf01,BOOLEAN,PLAIN,\n",
        "root.ln.wf04.wt04.status,root.ln.wf04,TEXT,PLAIN,\n"
            + "root.ln.wf04.wt05.temperature,root.ln.wf04,FLOAT,GORILLA,\n"
            + "root.ln.wf03.wt02.status,root.ln.wf03,INT64,PLAIN,\n"
            + "root.ln.wf03.wt03.temperature,root.ln.wf03,FLOAT,TS_2DIFF,\n"
            + "root.ln.wf02.wt03.status,root.ln.wf02,INT32,PLAIN,\n"
            + "root.ln.wf02.wt04.temperature,root.ln.wf02,FLOAT,RLE,\n"
            + "root.ln.wf01.wt01.status,root.ln.wf01,BOOLEAN,PLAIN,\n"
            + "root.ln.wf01.wt01.temperature,root.ln.wf01,FLOAT,RLE,\n"
            + "root.ln.wf01.wt02.humidity,root.ln.wf01,DOUBLE,RLE,\n"
            + "root.ln.wf05.wt01.status,root.ln.wf05,DOUBLE,PLAIN,\n",
        "root.ln.wf01.wt01.status,root.ln.wf01,BOOLEAN,PLAIN,\n"
            + "root.ln.wf01.wt01.temperature,root.ln.wf01,FLOAT,RLE,\n",
        "root.ln.wf01.wt01.status,root.ln.wf01,BOOLEAN,PLAIN,\n"
            + "root.ln.wf05.wt01.status,root.ln.wf05,DOUBLE,PLAIN,\n",
        "root.ln.wf01.wt01.status,root.ln.wf01,BOOLEAN,PLAIN,\n"
            + "root.ln.wf01.wt01.temperature,root.ln.wf01,FLOAT,RLE,\n"
            + "root.ln.wf05.wt01.status,root.ln.wf05,DOUBLE,PLAIN,\n",
        "",
        ""
    };
    checkCorrectness(sqls, standards, statement);
  }

  protected void testShowTimeseries(Connection connection) throws SQLException {
    Statement statement = connection.createStatement();
    try {
      statement.execute("show timeseries");
    } catch (SQLException e) {
      return;
    } catch (Exception e) {
      fail(e.getMessage());
    } finally {
      statement.close();
    }
  }

  protected void testDatabaseMetadata(Connection connection) throws SQLException {
    DatabaseMetaData databaseMetaData = connection.getMetaData();
    showTimeseriesInJson(databaseMetaData);
    showStorageGroup(databaseMetaData);
    showAllColumns(databaseMetaData);
    showDeltaObject(databaseMetaData);
    showTimeseriesInfo(databaseMetaData);
  }

  protected void checkCorrectness(String[] sqls, String[] standards, Statement statement)
      throws SQLException {
    for (int i = 0; i < sqls.length; i++) {
      String sql = sqls[i];
      String standard = standards[i];
      StringBuilder builder = new StringBuilder();
      try {
        boolean hasResultSet = statement.execute(sql);
        if (hasResultSet) {
          ResultSet resultSet = statement.getResultSet();
          ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
          while (resultSet.next()) {
            for (int j = 1; j <= resultSetMetaData.getColumnCount(); j++) {
              builder.append(resultSet.getString(j)).append(",");
            }
            builder.append("\n");
          }
        }
        Assert.assertEquals(standard, builder.toString());
      } catch (SQLException e) {
        e.printStackTrace();
        fail();
      } finally {
        statement.close();
      }
    }
  }

  protected void showTimeseriesInfo(DatabaseMetaData databaseMetaData) throws SQLException {
    String standard =
        "Timeseries,Storage Group,DataType,Encoding,\n"
            + "root.ln.wf04.wt04.status,root.ln.wf04,TEXT,PLAIN,\n"
            + "root.ln.wf04.wt05.temperature,root.ln.wf04,FLOAT,GORILLA,\n"
            + "root.ln.wf03.wt02.status,root.ln.wf03,INT64,PLAIN,\n"
            + "root.ln.wf03.wt03.temperature,root.ln.wf03,FLOAT,TS_2DIFF,\n"
            + "root.ln.wf02.wt03.status,root.ln.wf02,INT32,PLAIN,\n"
            + "root.ln.wf02.wt04.temperature,root.ln.wf02,FLOAT,RLE,\n"
            + "root.ln.wf01.wt01.status,root.ln.wf01,BOOLEAN,PLAIN,\n"
            + "root.ln.wf01.wt01.temperature,root.ln.wf01,FLOAT,RLE,\n"
            + "root.ln.wf01.wt02.humidity,root.ln.wf01,DOUBLE,RLE,\n"
            + "root.ln.wf05.wt01.status,root.ln.wf05,DOUBLE,PLAIN,\n";
    ResultSet resultSet = databaseMetaData
        .getColumns(Constant.CATALOG_TIMESERIES, "root", null, null);
    checkCorrectness(resultSet, standard);
    resultSet.close();

    resultSet = databaseMetaData.getColumns(Constant.CATALOG_TIMESERIES, "root.ln", null, null);
    checkCorrectness(resultSet, standard);
    resultSet.close();

    standard =
        "Timeseries,Storage Group,DataType,Encoding,\n"
            + "root.ln.wf01.wt01.status,root.ln.wf01,BOOLEAN,PLAIN,\n"
            + "root.ln.wf01.wt01.temperature,root.ln.wf01,FLOAT,RLE,\n"
            + "root.ln.wf01.wt02.humidity,root.ln.wf01,DOUBLE,RLE,\n";
    resultSet = databaseMetaData
        .getColumns(Constant.CATALOG_TIMESERIES, "root.ln.wf01", null, null);
    checkCorrectness(resultSet, standard);
    resultSet.close();

    standard =
        "Timeseries,Storage Group,DataType,Encoding,\n"
            + "root.ln.wf01.wt01.status,root.ln.wf01,BOOLEAN,PLAIN,\n"
            + "root.ln.wf01.wt01.temperature,root.ln.wf01,FLOAT,RLE,\n"
            + "root.ln.wf05.wt01.status,root.ln.wf05,DOUBLE,PLAIN,\n";
    resultSet = databaseMetaData
        .getColumns(Constant.CATALOG_TIMESERIES, "root.ln.*.wt01", null, null);
    checkCorrectness(resultSet, standard);
    resultSet.close();

    standard =
        "Timeseries,Storage Group,DataType,Encoding,\n"
            + "root.ln.wf01.wt01.status,root.ln.wf01,BOOLEAN,PLAIN,\n";
    resultSet = databaseMetaData
        .getColumns(Constant.CATALOG_TIMESERIES, "root.ln.wf01.wt01.status", null, null);
    checkCorrectness(resultSet, standard);
    resultSet.close();
  }

  protected void showTimeseriesInJson(DatabaseMetaData databaseMetaData) {
    String metadataInJson = databaseMetaData.toString();
    String standard =
        "===  Timeseries Tree  ===\n"
            + "\n"
            + "{\n"
            + "\t\"root\":{\n"
            + "\t\t\"ln\":{\n"
            + "\t\t\t\"wf05\":{\n"
            + "\t\t\t\t\"wt01\":{\n"
            + "\t\t\t\t\t\"status\":{\n"
            + "\t\t\t\t\t\t\"args\":\"{}\",\n"
            + "\t\t\t\t\t\t\"StorageGroup\":\"root.ln.wf05\",\n"
            + "\t\t\t\t\t\t\"DataType\":\"DOUBLE\",\n"
            + "\t\t\t\t\t\t\"Compressor\":\"UNCOMPRESSED\",\n"
            + "\t\t\t\t\t\t\"Encoding\":\"PLAIN\"\n"
            + "\t\t\t\t\t}\n"
            + "\t\t\t\t}\n"
            + "\t\t\t},\n"
            + "\t\t\t\"wf04\":{\n"
            + "\t\t\t\t\"wt05\":{\n"
            + "\t\t\t\t\t\"temperature\":{\n"
            + "\t\t\t\t\t\t\"args\":\"{}\",\n"
            + "\t\t\t\t\t\t\"StorageGroup\":\"root.ln.wf04\",\n"
            + "\t\t\t\t\t\t\"DataType\":\"FLOAT\",\n"
            + "\t\t\t\t\t\t\"Compressor\":\"UNCOMPRESSED\",\n"
            + "\t\t\t\t\t\t\"Encoding\":\"GORILLA\"\n"
            + "\t\t\t\t\t}\n"
            + "\t\t\t\t},\n"
            + "\t\t\t\t\"wt04\":{\n"
            + "\t\t\t\t\t\"status\":{\n"
            + "\t\t\t\t\t\t\"args\":\"{}\",\n"
            + "\t\t\t\t\t\t\"StorageGroup\":\"root.ln.wf04\",\n"
            + "\t\t\t\t\t\t\"DataType\":\"TEXT\",\n"
            + "\t\t\t\t\t\t\"Compressor\":\"UNCOMPRESSED\",\n"
            + "\t\t\t\t\t\t\"Encoding\":\"PLAIN\"\n"
            + "\t\t\t\t\t}\n"
            + "\t\t\t\t}\n"
            + "\t\t\t},\n"
            + "\t\t\t\"wf03\":{\n"
            + "\t\t\t\t\"wt03\":{\n"
            + "\t\t\t\t\t\"temperature\":{\n"
            + "\t\t\t\t\t\t\"args\":\"{max_point_number=5}\",\n"
            + "\t\t\t\t\t\t\"StorageGroup\":\"root.ln.wf03\",\n"
            + "\t\t\t\t\t\t\"DataType\":\"FLOAT\",\n"
            + "\t\t\t\t\t\t\"Compressor\":\"UNCOMPRESSED\",\n"
            + "\t\t\t\t\t\t\"Encoding\":\"TS_2DIFF\"\n"
            + "\t\t\t\t\t}\n"
            + "\t\t\t\t},\n"
            + "\t\t\t\t\"wt02\":{\n"
            + "\t\t\t\t\t\"status\":{\n"
            + "\t\t\t\t\t\t\"args\":\"{}\",\n"
            + "\t\t\t\t\t\t\"StorageGroup\":\"root.ln.wf03\",\n"
            + "\t\t\t\t\t\t\"DataType\":\"INT64\",\n"
            + "\t\t\t\t\t\t\"Compressor\":\"UNCOMPRESSED\",\n"
            + "\t\t\t\t\t\t\"Encoding\":\"PLAIN\"\n"
            + "\t\t\t\t\t}\n"
            + "\t\t\t\t}\n"
            + "\t\t\t},\n"
            + "\t\t\t\"wf02\":{\n"
            + "\t\t\t\t\"wt04\":{\n"
            + "\t\t\t\t\t\"temperature\":{\n"
            + "\t\t\t\t\t\t\"args\":\"{}\",\n"
            + "\t\t\t\t\t\t\"StorageGroup\":\"root.ln.wf02\",\n"
            + "\t\t\t\t\t\t\"DataType\":\"FLOAT\",\n"
            + "\t\t\t\t\t\t\"Compressor\":\"UNCOMPRESSED\",\n"
            + "\t\t\t\t\t\t\"Encoding\":\"RLE\"\n"
            + "\t\t\t\t\t}\n"
            + "\t\t\t\t},\n"
            + "\t\t\t\t\"wt03\":{\n"
            + "\t\t\t\t\t\"status\":{\n"
            + "\t\t\t\t\t\t\"args\":\"{}\",\n"
            + "\t\t\t\t\t\t\"StorageGroup\":\"root.ln.wf02\",\n"
            + "\t\t\t\t\t\t\"DataType\":\"INT32\",\n"
            + "\t\t\t\t\t\t\"Compressor\":\"UNCOMPRESSED\",\n"
            + "\t\t\t\t\t\t\"Encoding\":\"PLAIN\"\n"
            + "\t\t\t\t\t}\n"
            + "\t\t\t\t}\n"
            + "\t\t\t},\n"
            + "\t\t\t\"wf01\":{\n"
            + "\t\t\t\t\"wt02\":{\n"
            + "\t\t\t\t\t\"humidity\":{\n"
            + "\t\t\t\t\t\t\"args\":\"{}\",\n"
            + "\t\t\t\t\t\t\"StorageGroup\":\"root.ln.wf01\",\n"
            + "\t\t\t\t\t\t\"DataType\":\"DOUBLE\",\n"
            + "\t\t\t\t\t\t\"Compressor\":\"UNCOMPRESSED\",\n"
            + "\t\t\t\t\t\t\"Encoding\":\"RLE\"\n"
            + "\t\t\t\t\t}\n"
            + "\t\t\t\t},\n"
            + "\t\t\t\t\"wt01\":{\n"
            + "\t\t\t\t\t\"temperature\":{\n"
            + "\t\t\t\t\t\t\"args\":\"{max_point_number=3}\",\n"
            + "\t\t\t\t\t\t\"StorageGroup\":\"root.ln.wf01\",\n"
            + "\t\t\t\t\t\t\"DataType\":\"FLOAT\",\n"
            + "\t\t\t\t\t\t\"Compressor\":\"SNAPPY\",\n"
            + "\t\t\t\t\t\t\"Encoding\":\"RLE\"\n"
            + "\t\t\t\t\t},\n"
            + "\t\t\t\t\t\"status\":{\n"
            + "\t\t\t\t\t\t\"args\":\"{}\",\n"
            + "\t\t\t\t\t\t\"StorageGroup\":\"root.ln.wf01\",\n"
            + "\t\t\t\t\t\t\"DataType\":\"BOOLEAN\",\n"
            + "\t\t\t\t\t\t\"Compressor\":\"UNCOMPRESSED\",\n"
            + "\t\t\t\t\t\t\"Encoding\":\"PLAIN\"\n"
            + "\t\t\t\t\t}\n"
            + "\t\t\t\t}\n"
            + "\t\t\t}\n"
            + "\t\t}\n"
            + "\t}\n"
            + "}";

    Assert.assertEquals(standard, metadataInJson);
  }

  protected void showStorageGroup(DatabaseMetaData databaseMetaData) throws SQLException {
    String standard =
        "Storage Group,\n"
            + "root.ln.wf04,\n"
            + "root.ln.wf03,\n"
            + "root.ln.wf02,\n"
            + "root.ln.wf01,\n"
            + "root.ln.wf05,\n";
    ResultSet resultSet = databaseMetaData
        .getColumns(Constant.CATALOG_STORAGE_GROUP, null, null, null);
    checkCorrectness(resultSet, standard);
  }

  protected void showDeltaObject(DatabaseMetaData databaseMetaData) throws SQLException {
    String standard =
        "Column,\n"
            + "root.ln.wf02.wt04,\n"
            + "root.ln.wf02.wt03,\n"
            + "root.ln.wf04.wt05,\n"
            + "root.ln.wf04.wt04,\n"
            + "root.ln.wf01.wt01,\n"
            + "root.ln.wf05.wt01,\n"
            + "root.ln.wf01.wt02,\n"
            + "root.ln.wf03.wt03,\n"
            + "root.ln.wf03.wt02,\n";
    ResultSet resultSet = databaseMetaData.getColumns(Constant.CATALOG_DEVICE, "ln", null, null);
    checkCorrectness(resultSet, standard);
    resultSet.close();

    standard = "Column,\n";
    resultSet = databaseMetaData.getColumns(Constant.CATALOG_DEVICE, "ln1", null, null);
    checkCorrectness(resultSet, standard);
    resultSet.close();
  }

  protected void showAllColumns(DatabaseMetaData databaseMetaData) throws SQLException {
    String standard =
        "Column,\n"
            + "root.ln.wf04.wt04.status,\n"
            + "root.ln.wf04.wt05.temperature,\n"
            + "root.ln.wf03.wt02.status,\n"
            + "root.ln.wf03.wt03.temperature,\n"
            + "root.ln.wf02.wt03.status,\n"
            + "root.ln.wf02.wt04.temperature,\n"
            + "root.ln.wf01.wt01.status,\n"
            + "root.ln.wf01.wt01.temperature,\n"
            + "root.ln.wf01.wt02.humidity,\n"
            + "root.ln.wf05.wt01.status,\n";
    ResultSet resultSet = databaseMetaData.getColumns(Constant.CATALOG_COLUMN, "root", null, null);
    checkCorrectness(resultSet, standard);
    resultSet.close();

    standard =
        "Column,\n"
            + "root.ln.wf01.wt01.status,\n"
            + "root.ln.wf01.wt01.temperature,\n"
            + "root.ln.wf01.wt02.humidity,\n";
    resultSet = databaseMetaData.getColumns(Constant.CATALOG_COLUMN, "root.ln.wf01", null, null);
    checkCorrectness(resultSet, standard);
    resultSet.close();

    standard =
        "Column,\n"
            + "root.ln.wf03.wt02.status,\n"
            + "root.ln.wf01.wt02.humidity,\n";
    resultSet = databaseMetaData.getColumns(Constant.CATALOG_COLUMN, "root.ln.*.wt02", null, null);
    checkCorrectness(resultSet, standard);
    resultSet.close();
  }

  protected void checkCorrectness(ResultSet resultSet, String standard) throws SQLException {
    ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
    int colCount = resultSetMetaData.getColumnCount();
    StringBuilder resultStr = new StringBuilder();
    for (int i = 1; i < colCount + 1; i++) {
      resultStr.append(resultSetMetaData.getColumnName(i)).append(",");
    }
    resultStr.append("\n");
    while (resultSet.next()) {
      for (int i = 1; i <= colCount; i++) {
        resultStr.append(resultSet.getString(i)).append(",");
      }
      resultStr.append("\n");
    }
    Assert.assertEquals(standard, resultStr.toString());
  }
}
