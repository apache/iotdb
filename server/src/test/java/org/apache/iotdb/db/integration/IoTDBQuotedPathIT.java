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

import static org.apache.iotdb.db.constant.TestConstant.TIMESTAMP_STR;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.conf.directories.DirectoryManager;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.jdbc.Config;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class IoTDBQuotedPathIT {

  @Before
  public void setUp() throws Exception {
    EnvironmentUtils.closeStatMonitor();
    EnvironmentUtils.envSetUp();
    Class.forName(Config.JDBC_DRIVER_NAME);
  }

  @After
  public void tearDown() throws Exception {
    EnvironmentUtils.cleanEnv();
  }

  @Test
  public void test() throws SQLException {
    try(Connection connection = DriverManager
            .getConnection(Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root",
                    "root");
        Statement statement = connection.createStatement();){
      String[] exp = new String[]{
              "1509465600000,true",
              "1509465600001,true",
              "1509465600002,false",
              "1509465600003,false"
      };
      statement.execute("SET STORAGE GROUP TO root.\"ln.sg\".wf01.wt01");
      statement.execute("CREATE TIMESERIES root.\"ln.sg\".wf01.wt01.\"status.2.3\" WITH DATATYPE=BOOLEAN, ENCODING=PLAIN");
      statement.execute("INSERT INTO root.\"ln.sg\".wf01.wt01(timestamp,\"status.2.3\") values(1509465600000,true)");
      statement.execute("INSERT INTO root.\"ln.sg\".wf01.wt01(timestamp,\"status.2.3\") values(1509465600001,true)");
      statement.execute("INSERT INTO root.\"ln.sg\".wf01.wt01(timestamp,\"status.2.3\") values(1509465600002,false)");
      statement.execute("INSERT INTO root.\"ln.sg\".wf01.wt01(timestamp,\"status.2.3\") values(1509465600003,false)");
      statement.execute("SET STORAGE GROUP TO root.\"ln.sg\".wf01.wt02");
      statement.execute("CREATE TIMESERIES root.\"ln.sg\".wf01.wt02.\"abd\" WITH DATATYPE=BOOLEAN, ENCODING=PLAIN");
      statement.execute("CREATE TIMESERIES root.\"ln.sg\".wf01.wt02.\"asf.asd.sdf\" WITH DATATYPE=BOOLEAN, ENCODING=PLAIN");
      statement.execute("CREATE TIMESERIES root.\"ln.sg\".wf01.wt02.\"asd12\" WITH DATATYPE=BOOLEAN, ENCODING=PLAIN");
      boolean hasResultSet = statement.execute("SELECT * FROM root.\"ln.sg\".wf01.wt01");

      assertTrue(hasResultSet);
      int cnt;
      ArrayList<String> ans = new ArrayList<>();
      ResultSet resultSet = statement.getResultSet();
      cnt = 0;
      while (resultSet.next()) {
        String result = resultSet.getString(TIMESTAMP_STR) + "," + resultSet.getString(2);
        ans.add(result);
        cnt++;
      }

      for (int i = 0; i < ans.size(); i++) {
        assertEquals(exp[i], ans.get(i));
      }

      hasResultSet = statement.execute("SELECT  * FROM root.\"ln.sg\".wf01.wt01 WHERE \"status.2.3\" = false");
      assertTrue(hasResultSet);
      exp = new String[]{
              "1509465600002,false",
              "1509465600003,false"
      };
      ans = new ArrayList<>();
      resultSet = statement.getResultSet();
      cnt = 0;
      while (resultSet.next()) {
        String result = resultSet.getString(TIMESTAMP_STR) + "," + resultSet.getString(2);
        ans.add(result);
        cnt++;
      }

      for (int i = 0; i < exp.length; i++) {
        assertEquals(exp[i], ans.get(i));
      }
      System.out.println(Arrays.toString(IoTDBDescriptor.getInstance().getConfig().getDataDirs()));
      for(String s: IoTDBDescriptor.getInstance().getConfig().getDataDirs()) {
        printFile(s);
      }
      for(String s: DirectoryManager.getInstance().getAllUnSequenceFileFolders()) {
        printFile(s);
      }
      for(String s: DirectoryManager.getInstance().getAllSequenceFileFolders()) {
        printFile(s);
      }
      statement.execute("DELETE FROM root.\"ln.sg\".wf01.wt01.\"status.2.3\" WHERE time < 1509465600001");
      statement.execute("DELETE TIMESERIES root.\"ln.sg\".wf01.wt01.\"status.2.3\"");
    } catch (Exception e) {
      System.out.println(Arrays.toString(IoTDBDescriptor.getInstance().getConfig().getDataDirs()));
      for(String s: IoTDBDescriptor.getInstance().getConfig().getDataDirs()) {
        printFile(s);
      }
      for(String s: DirectoryManager.getInstance().getAllUnSequenceFileFolders()) {
        printFile(s);
      }
      for(String s: DirectoryManager.getInstance().getAllSequenceFileFolders()) {
        printFile(s);
      }
      e.printStackTrace();
    }
  }

  private void printFile(String path) {
    File folder = new File(path);
    File[] listOfFiles = folder.listFiles();

    if (listOfFiles != null) {
      for (File listOfFile : listOfFiles) {
        if (listOfFile.isFile()) {
          System.out.println("File " + listOfFile.getName());
        } else if (listOfFile.isDirectory()) {
          System.out.println("Directory " + listOfFile.getName());
          printFile(listOfFile.getPath());
        }
      }
    }
  }
}
