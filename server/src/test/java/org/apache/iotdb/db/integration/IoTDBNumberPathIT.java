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
package org.apache.iotdb.db.integration;

import org.apache.iotdb.db.constant.TestConstant;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.jdbc.Config;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;

/**
 * Notice that, all test begins with "IoTDB" is integration test. All test which will start the
 * IoTDB server should be defined as integration test.
 */
public class IoTDBNumberPathIT {

  @Before
  public void setUp() {
    EnvironmentUtils.closeStatMonitor();
    EnvironmentUtils.envSetUp();
  }

  @After
  public void tearDown() throws Exception {
    EnvironmentUtils.cleanEnv();
  }

  @Test
  public void test() throws ClassNotFoundException {
    String[] sqls = {"SET STORAGE GROUP TO root.123"};
    executeSQL(sqls);
    // simpleTest();
    //    insertTest();
    selectTest();
    //    deleteTest();
    //    groupByTest();
    //    funcTest();

    //    funcTestWithOutTimeGenerator();
  }

  public void simpleTest() throws ClassNotFoundException {
    String[] sqlS = {
      "CREATE TIMESERIES root.123.456.789 WITH DATATYPE=INT32,ENCODING=RLE",
      "SHOW TIMESERIES",
      "===  Timeseries Tree  ===\n"
          + "\n"
          + "{\n"
          + "\t\"root\":{\n"
          + "\t\t\"123\":{\n"
          + "\t\t\t\"456\":{\n"
          + "\t\t\t\t\"789\":{\n"
          + "\t\t\t\t\t\"args\":\"{}\",\n"
          + "\t\t\t\t\t\"StorageGroup\":\"root.123\",\n"
          + "\t\t\t\t\t\"DataType\":\"INT32\",\n"
          + "\t\t\t\t\t\"Compressor\":\"UNCOMPRESSED\",\n"
          + "\t\t\t\t\t\"Encoding\":\"RLE\"\n"
          + "\t\t\t\t}\n"
          + "\t\t\t}\n"
          + "\t\t}\n"
          + "\t}\n"
          + "}",
      "DELETE TIMESERIES root.123.456.789",
      "SHOW TIMESERIES",
      "===  Timeseries Tree  ===\n"
          + "\n"
          + "{\n"
          + "\t\"root\":{\n"
          + "\t\t\"123\":{}\n"
          + "\t}\n"
          + "}",
      "CREATE TIMESERIES root.123.456.789 WITH DATATYPE=BOOLEAN,ENCODING=PLAIN",
      "CREATE TIMESERIES root.123.456.000 WITH DATATYPE=INT64,ENCODING=TS_2DIFF",
      "CREATE TIMESERIES root.123.456.1111 WITH DATATYPE=FLOAT,ENCODING=GORILLA",
      "CREATE TIMESERIES root.123.456.4444 WITH DATATYPE=DOUBLE,ENCODING=RLE",
      "CREATE TIMESERIES root.123.d1.555 WITH DATATYPE=TEXT,ENCODING=PLAIN",
      "CREATE TIMESERIES root.123.d2.666 WITH DATATYPE=INT32,ENCODING=TS_2DIFF,compressor=UNCOMPRESSED",
      "CREATE TIMESERIES root.123.d3.77777 WITH DATATYPE=INT32,ENCODING=RLE,compressor=SNAPPY",
      "CREATE TIMESERIES root.123.d4.888 WITH DATATYPE=INT32,ENCODING=RLE,MAX_POINT_NUMBER=100",
      "CREATE TIMESERIES root.123.d5.s9 WITH DATATYPE=FLOAT,ENCODING=PLAIN,compressor=SNAPPY,MAX_POINT_NUMBER=10",
      "CREATE TIMESERIES root.123.d6.0000 WITH DATATYPE=DOUBLE,ENCODING=RLE,compressor=UNCOMPRESSED,MAX_POINT_NUMBER=10",
      "DELETE TIMESERIES root.123.456.*",
      "SHOW TIMESERIES",
      "===  Timeseries Tree  ===\n"
          + "\n"
          + "{\n"
          + "\t\"root\":{\n"
          + "\t\t\"123\":{\n"
          + "\t\t\t\"d4\":{\n"
          + "\t\t\t\t\"888\":{\n"
          + "\t\t\t\t\t\"args\":\"{max_point_number=100}\",\n"
          + "\t\t\t\t\t\"StorageGroup\":\"root.123\",\n"
          + "\t\t\t\t\t\"DataType\":\"INT32\",\n"
          + "\t\t\t\t\t\"Compressor\":\"UNCOMPRESSED\",\n"
          + "\t\t\t\t\t\"Encoding\":\"RLE\"\n"
          + "\t\t\t\t}\n"
          + "\t\t\t},\n"
          + "\t\t\t\"d5\":{\n"
          + "\t\t\t\t\"s9\":{\n"
          + "\t\t\t\t\t\"args\":\"{max_point_number=10}\",\n"
          + "\t\t\t\t\t\"StorageGroup\":\"root.123\",\n"
          + "\t\t\t\t\t\"DataType\":\"FLOAT\",\n"
          + "\t\t\t\t\t\"Compressor\":\"SNAPPY\",\n"
          + "\t\t\t\t\t\"Encoding\":\"PLAIN\"\n"
          + "\t\t\t\t}\n"
          + "\t\t\t},\n"
          + "\t\t\t\"d6\":{\n"
          + "\t\t\t\t\"0000\":{\n"
          + "\t\t\t\t\t\"args\":\"{max_point_number=10}\",\n"
          + "\t\t\t\t\t\"StorageGroup\":\"root.123\",\n"
          + "\t\t\t\t\t\"DataType\":\"DOUBLE\",\n"
          + "\t\t\t\t\t\"Compressor\":\"UNCOMPRESSED\",\n"
          + "\t\t\t\t\t\"Encoding\":\"RLE\"\n"
          + "\t\t\t\t}\n"
          + "\t\t\t},\n"
          + "\t\t\t\"d1\":{\n"
          + "\t\t\t\t\"555\":{\n"
          + "\t\t\t\t\t\"args\":\"{}\",\n"
          + "\t\t\t\t\t\"StorageGroup\":\"root.123\",\n"
          + "\t\t\t\t\t\"DataType\":\"TEXT\",\n"
          + "\t\t\t\t\t\"Compressor\":\"UNCOMPRESSED\",\n"
          + "\t\t\t\t\t\"Encoding\":\"PLAIN\"\n"
          + "\t\t\t\t}\n"
          + "\t\t\t},\n"
          + "\t\t\t\"d2\":{\n"
          + "\t\t\t\t\"666\":{\n"
          + "\t\t\t\t\t\"args\":\"{}\",\n"
          + "\t\t\t\t\t\"StorageGroup\":\"root.123\",\n"
          + "\t\t\t\t\t\"DataType\":\"INT32\",\n"
          + "\t\t\t\t\t\"Compressor\":\"UNCOMPRESSED\",\n"
          + "\t\t\t\t\t\"Encoding\":\"TS_2DIFF\"\n"
          + "\t\t\t\t}\n"
          + "\t\t\t},\n"
          + "\t\t\t\"d3\":{\n"
          + "\t\t\t\t\"77777\":{\n"
          + "\t\t\t\t\t\"args\":\"{}\",\n"
          + "\t\t\t\t\t\"StorageGroup\":\"root.123\",\n"
          + "\t\t\t\t\t\"DataType\":\"INT32\",\n"
          + "\t\t\t\t\t\"Compressor\":\"SNAPPY\",\n"
          + "\t\t\t\t\t\"Encoding\":\"RLE\"\n"
          + "\t\t\t\t}\n"
          + "\t\t\t}\n"
          + "\t\t}\n"
          + "\t}\n"
          + "}",
      "DELETE TIMESERIES root.123.*",
      "SHOW TIMESERIES",
      "===  Timeseries Tree  ===\n"
          + "\n"
          + "{\n"
          + "\t\"root\":{\n"
          + "\t\t\"123\":{}\n"
          + "\t}\n"
          + "}"
    };
    executeSQL(sqlS);
  }

  public void insertTest() throws ClassNotFoundException {
    String[] sqlS = {
      "CREATE TIMESERIES root.123.456.789 WITH DATATYPE=INT32,ENCODING=RLE",
      "INSERT INTO root.123.456(timestamp,789) values(1,101)",
      "CREATE TIMESERIES root.123.456.000 WITH DATATYPE=INT32,ENCODING=RLE",
      "INSERT INTO root.123.456(timestamp,789,000) values(2,102,202)",
      "INSERT INTO root.123.456(timestamp,789) values(NOW(),104)",
      "INSERT INTO root.123.456(timestamp,789) values(2000-01-01T08:00:00+08:00,105)",
      "SELECT * FROM root.123.456",
      "1,101,null,\n" + "2,102,202,\n" + "946684800000,105,null,\n" + "NOW(),104,null,\n",
      "DELETE TIMESERIES root.123.*"
    };
    executeSQL(sqlS);
  }

  public void deleteTest() throws ClassNotFoundException {
    String[] sqlS = {
      "CREATE TIMESERIES root.123.456.789 WITH DATATYPE=INT32,ENCODING=RLE",
      "INSERT INTO root.123.456(timestamp,789) values(1,101)",
      "CREATE TIMESERIES root.123.456.000 WITH DATATYPE=INT32,ENCODING=RLE",
      "INSERT INTO root.123.456(timestamp,789,000) values(2,102,202)",
      "INSERT INTO root.123.456(timestamp,789) values(NOW(),104)",
      "INSERT INTO root.123.456(timestamp,789) values(2000-01-01T08:00:00+08:00,105)",
      "SELECT * FROM root.123.456",
      "1,101,null,\n" + "2,102,202,\n" + "946684800000,105,null,\n" + "NOW(),104,null,\n",
      "DELETE TIMESERIES root.123.*",
      "CREATE TIMESERIES root.123.456.789 WITH DATATYPE=INT32,ENCODING=RLE",
      "INSERT INTO root.123.456(timestamp,789) values(1,1)",
      "INSERT INTO root.123.456(timestamp,789) values(2,1)",
      "INSERT INTO root.123.456(timestamp,789) values(3,1)",
      "INSERT INTO root.123.456(timestamp,789) values(4,1)",
      "INSERT INTO root.123.456(timestamp,789) values(5,1)",
      "INSERT INTO root.123.456(timestamp,789) values(6,1)",
      "INSERT INTO root.123.456(timestamp,789) values(7,1)",
      "INSERT INTO root.123.456(timestamp,789) values(8,1)",
      "INSERT INTO root.123.456(timestamp,789) values(9,1)",
      "INSERT INTO root.123.456(timestamp,789) values(10,1)",
      "SELECT * FROM root.123.456",
      "1,1,\n" + "2,1,\n" + "3,1,\n" + "4,1,\n" + "5,1,\n" + "6,1,\n" + "7,1,\n" + "8,1,\n"
          + "9,1,\n" + "10,1,\n",
      "DELETE FROM root.123.456.789 WHERE time < 8",
      "SELECT * FROM root.123.456",
      "8,1,\n" + "9,1,\n" + "10,1,\n",
      "INSERT INTO root.123.456(timestamp,789) values(2000-01-01T08:00:00+08:00,1)",
      "SELECT * FROM root.123.456",
      "8,1,\n" + "9,1,\n" + "10,1,\n" + "946684800000,1,\n",
      "DELETE FROM root.123.456.789 WHERE time < 2000-01-02T08:00:00+08:00",
      "SELECT * FROM root.123.456",
      "",
      "INSERT INTO root.123.456(timestamp,789) values(NOW(),1)",
      "SELECT * FROM root.123.456",
      "NOW(),1,\n",
      "DELETE FROM root.123.456.789 WHERE time <= NOW()",
      "SELECT * FROM root.123.456",
      "",
      "CREATE TIMESERIES root.123.d1.000 WITH DATATYPE=INT32,ENCODING=RLE",
      "INSERT INTO root.123.456(timestamp,789) values(1,1)",
      "INSERT INTO root.123.d1(timestamp,000) values(1,1)",
      "INSERT INTO root.123.456(timestamp,789) values(5,5)",
      "INSERT INTO root.123.d1(timestamp,000) values(5,5)",
      "SELECT * FROM root.123",
      "1,1,1,\n" + "5,5,5,\n",
      "DELETE FROM root.123.456.789,root.123.d1.000 WHERE time < 3",
      "SELECT * FROM root.123",
      "5,5,5,\n",
      "DELETE FROM root.123.* WHERE time < 7",
      "SELECT * FROM root.123",
      "",
      "DELETE TIMESERIES root.123.*"
    };
    executeSQL(sqlS);
  }

  public void selectTest() throws ClassNotFoundException {
    String[] sqlS = {
      "CREATE TIMESERIES root.123.456.789 WITH DATATYPE=INT32,ENCODING=RLE",
      "INSERT INTO root.123.456(timestamp,789) values(1,101)",
      "INSERT INTO root.123.456(timestamp,789) values(2,102)",
      "INSERT INTO root.123.456(timestamp,789) values(3,103)",
      "INSERT INTO root.123.456(timestamp,789) values(4,104)",
      "INSERT INTO root.123.456(timestamp,789) values(5,105)",
      "INSERT INTO root.123.456(timestamp,789) values(6,106)",
      "INSERT INTO root.123.456(timestamp,789) values(7,107)",
      "INSERT INTO root.123.456(timestamp,789) values(8,108)",
      "INSERT INTO root.123.456(timestamp,789) values(9,109)",
      "INSERT INTO root.123.456(timestamp,789) values(10,110)",
      "SELECT * FROM root.123.456 WHERE 789 < 104",
      "1,101,\n" + "2,102,\n" + "3,103,\n",
      "SELECT * FROM root.123.456 WHERE 789 > 105 and time < 8",
      "6,106,\n" + "7,107,\n",
      "SELECT * FROM root.123.456",
      "1,101,\n"
          + "2,102,\n"
          + "3,103,\n"
          + "4,104,\n"
          + "5,105,\n"
          + "6,106,\n"
          + "7,107,\n"
          + "8,108,\n"
          + "9,109,\n"
          + "10,110,\n",
      "DELETE TIMESERIES root.123.*"
    };
    executeSQL(sqlS);
  }

  public void funcTest() throws ClassNotFoundException {
    String[] sqlS = {
      "CREATE TIMESERIES root.123.456.789 WITH DATATYPE=INT32,ENCODING=RLE",
      "INSERT INTO root.123.456(timestamp,789) values(1,110)",
      "INSERT INTO root.123.456(timestamp,789) values(2,109)",
      "INSERT INTO root.123.456(timestamp,789) values(3,108)",
      "INSERT INTO root.123.456(timestamp,789) values(4,107)",
      "INSERT INTO root.123.456(timestamp,789) values(5,106)",
      "INSERT INTO root.123.456(timestamp,789) values(6,105)",
      "INSERT INTO root.123.456(timestamp,789) values(7,104)",
      "INSERT INTO root.123.456(timestamp,789) values(8,103)",
      "INSERT INTO root.123.456(timestamp,789) values(9,102)",
      "INSERT INTO root.123.456(timestamp,789) values(10,101)",
      "SELECT COUNT(789) FROM root.123.456",
      "0,10,\n",
      "SELECT COUNT(789) FROM root.123.456 WHERE root.123.456.789 < 105",
      "0,4,\n",
      "SELECT MAX_TIME(789) FROM root.123.456",
      "0,10,\n",
      "SELECT MAX_TIME(789) FROM root.123.456 WHERE root.123.456.789 > 105",
      "0,5,\n",
      "SELECT MIN_TIME(789) FROM root.123.456",
      "0,1,\n",
      "SELECT MIN_TIME(789) FROM root.123.456 WHERE root.123.456.789 < 106",
      "0,6,\n",
      "SELECT MAX_VALUE(789) FROM root.123.456",
      "0,110,\n",
      "SELECT MAX_VALUE(789) FROM root.123.456 WHERE time > 4",
      "0,106,\n",
      "SELECT MIN_VALUE(789) FROM root.123.456",
      "0,101,\n",
      "SELECT MIN_VALUE(789) FROM root.123.456 WHERE time < 5",
      "0,107,\n",
      "DELETE FROM root.123.456.789 WHERE time <= 10",
      "INSERT INTO root.123.456(timestamp,789) values(NOW(),5)",
      "SELECT * FROM root.123.456",
      "NOW(),5,\n",
      "UPDATE root.123.456 SET 789 = 10 WHERE time <= NOW()",
      "SELECT * FROM root.123.456",
      "NOW(),10,\n",
      "DELETE FROM root.123.456.789 WHERE time <= NOW()",
      "SELECT * FROM root.123.456",
      "",
      "DELETE TIMESERIES root.123.*"
    };
    executeSQL(sqlS);
  }

  public void funcTestWithOutTimeGenerator() throws ClassNotFoundException {
    String[] sqlS = {
      "CREATE TIMESERIES root.123.456.789 WITH DATATYPE=INT32,ENCODING=RLE",
      "INSERT INTO root.123.456(timestamp,789) values(1,110)",
      "INSERT INTO root.123.456(timestamp,789) values(2,109)",
      "INSERT INTO root.123.456(timestamp,789) values(3,108)",
      "INSERT INTO root.123.456(timestamp,789) values(4,107)",
      "INSERT INTO root.123.456(timestamp,789) values(5,106)",
      "INSERT INTO root.123.456(timestamp,789) values(6,105)",
      "INSERT INTO root.123.456(timestamp,789) values(7,104)",
      "INSERT INTO root.123.456(timestamp,789) values(8,103)",
      "INSERT INTO root.123.456(timestamp,789) values(9,102)",
      "INSERT INTO root.123.456(timestamp,789) values(10,101)",
      "SELECT COUNT(789) FROM root.123.456",
      "0,10,\n",
      "SELECT MAX_TIME(789) FROM root.123.456",
      "0,10,\n",
      "SELECT MIN_TIME(789) FROM root.123.456",
      "0,1,\n",
      "SELECT MAX_VALUE(789) FROM root.123.456",
      "0,110,\n",
      "SELECT MAX_VALUE(789) FROM root.123.456 WHERE time > 4",
      "0,106,\n",
      "SELECT SUM(789) FROM root.123.456 WHERE time > 4",
      "0,621.0,\n",
      "SELECT MIN_VALUE(789) FROM root.123.456",
      "0,101,\n",
      "SELECT MIN_VALUE(789) FROM root.123.456 WHERE time < 5",
      "0,107,\n",
      "DELETE FROM root.123.456.789 WHERE time <= 10",
      "INSERT INTO root.123.456(timestamp,789) values(NOW(),5)",
      "SELECT * FROM root.123.456",
      "NOW(),5,\n",
      "DELETE FROM root.123.456.789 WHERE time <= NOW()",
      "SELECT * FROM root.123.456",
      "",
      "SELECT COUNT(789) FROM root.123.456",
      "0,0,\n",
      "SELECT MAX_TIME(789) FROM root.123.456",
      "",
      "SELECT MIN_TIME(789) FROM root.123.456",
      "",
      "SELECT MAX_VALUE(789) FROM root.123.456",
      "",
      "SELECT MAX_VALUE(789) FROM root.123.456 WHERE time > 4",
      "",
      "SELECT MIN_VALUE(789) FROM root.123.456",
      "",
      "SELECT MIN_VALUE(789) FROM root.123.456 WHERE time < 5",
      "",
      "DELETE TIMESERIES root.123.*"
    };
    executeSQL(sqlS);
  }

  public void groupByTest() throws ClassNotFoundException {
    String[] sqlS = {
      "CREATE TIMESERIES root.123.456.789 WITH DATATYPE=INT32,ENCODING=RLE",
      "INSERT INTO root.123.456(timestamp,789) values(1,110)",
      "INSERT INTO root.123.456(timestamp,789) values(2,109)",
      "INSERT INTO root.123.456(timestamp,789) values(3,108)",
      "INSERT INTO root.123.456(timestamp,789) values(4,107)",
      "INSERT INTO root.123.456(timestamp,789) values(5,106)",
      "INSERT INTO root.123.456(timestamp,789) values(6,105)",
      "INSERT INTO root.123.456(timestamp,789) values(7,104)",
      "INSERT INTO root.123.456(timestamp,789) values(8,103)",
      "INSERT INTO root.123.456(timestamp,789) values(9,102)",
      "INSERT INTO root.123.456(timestamp,789) values(10,101)",
      "CREATE TIMESERIES root.123.456.000 WITH DATATYPE=INT32,ENCODING=RLE",
      "INSERT INTO root.123.456(timestamp,000) values(1,101)",
      "INSERT INTO root.123.456(timestamp,000) values(2,102)",
      "INSERT INTO root.123.456(timestamp,000) values(3,103)",
      "INSERT INTO root.123.456(timestamp,000) values(4,104)",
      "INSERT INTO root.123.456(timestamp,000) values(5,105)",
      "INSERT INTO root.123.456(timestamp,000) values(6,106)",
      "INSERT INTO root.123.456(timestamp,000) values(7,107)",
      "INSERT INTO root.123.456(timestamp,000) values(8,108)",
      "INSERT INTO root.123.456(timestamp,000) values(9,109)",
      "INSERT INTO root.123.456(timestamp,000) values(10,110)",
      "SELECT COUNT(789), COUNT(000) FROM root.123.456 WHERE 000 < 109 GROUP BY(4ms,[1,10])",
      "1,3,3,\n" + "4,4,4,\n" + "8,1,1,\n",
      "SELECT COUNT(789), MAX_VALUE(000) FROM root.123.456 WHERE time < 7 GROUP BY(3ms,2,[1,5])",
      "1,1,101,\n" + "2,3,104,\n" + "5,1,105,\n",
      "SELECT MIN_VALUE(789), MAX_TIME(000) FROM root.123.456 WHERE 000 > 102 and time < 9 GROUP BY(3ms,1,[1,4],[6,9])",
      "1,108,3,\n" + "4,105,6,\n" + "7,103,8,\n",
      "DELETE TIMESERIES root.123.*"
    };
    executeSQL(sqlS);
  }

  private void executeSQL(String[] sqls) throws ClassNotFoundException {
    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      String result = "";
      Long now_start = 0L;
      boolean cmp = false;
      for (String sql : sqls) {
        if (cmp) {
          Assert.assertEquals(sql, result);
          cmp = false;
        } else if (sql.equals("SHOW TIMESERIES")) {
          DatabaseMetaData data = connection.getMetaData();
          result = data.toString();
          cmp = true;
        } else {
          if (sql.contains("NOW()") && now_start == 0L) {
            now_start = System.currentTimeMillis();
          }
          statement.execute(sql);
          if (sql.split(" ")[0].equals("SELECT")) {
            ResultSet resultSet = statement.getResultSet();
            ResultSetMetaData metaData = resultSet.getMetaData();
            int count = metaData.getColumnCount();
            String[] column = new String[count];
            for (int i = 0; i < count; i++) {
              column[i] = metaData.getColumnName(i + 1);
            }
            result = "";
            while (resultSet.next()) {
              for (int i = 1; i <= count; i++) {
                if (now_start > 0L && column[i - 1] == TestConstant.TIMESTAMP_STR) {
                  String timestr = resultSet.getString(i);
                  Long tn = Long.valueOf(timestr);
                  Long now = System.currentTimeMillis();
                  if (tn >= now_start && tn <= now) {
                    timestr = "NOW()";
                  }
                  result += timestr + ',';
                } else {
                  result += resultSet.getString(i) + ',';
                }
              }
              result += '\n';
            }
            cmp = true;
          }
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
