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
package org.apache.iotdb.db.integration.sync;

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.newsync.receiver.load.DeletionPlanLoader;
import org.apache.iotdb.db.newsync.receiver.load.ILoader;
import org.apache.iotdb.db.newsync.receiver.load.TsFileLoader;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.itbase.category.LocalStandaloneTest;
import org.apache.iotdb.jdbc.Config;
import org.apache.iotdb.tsfile.utils.FilePathUtils;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.sql.*;
import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

@Category({LocalStandaloneTest.class})
public class IoTDBSyncReceiverLoaderIT {
  private static final Logger LOGGER = LoggerFactory.getLogger(IoTDBSyncReceiverLoaderIT.class);
  protected static boolean enableSeqSpaceCompaction;
  protected static boolean enableUnseqSpaceCompaction;
  protected static boolean enableCrossSpaceCompaction;
  /** create tsfile and move to tmpDir for sync test */
  File tmpDir = new File("target/synctest");

  private static final String[] schemaSqls =
      new String[] {
        "SET STORAGE GROUP TO root.vehicle",
        "CREATE TIMESERIES root.vehicle.d0.s0 WITH DATATYPE=INT32, ENCODING=RLE",
        "CREATE TIMESERIES root.vehicle.d0.s1 WITH DATATYPE=TEXT, ENCODING=PLAIN",
        "CREATE TIMESERIES root.vehicle.d1.s2 WITH DATATYPE=FLOAT, ENCODING=RLE",
        "CREATE TIMESERIES root.vehicle.d1.s3 WITH DATATYPE=BOOLEAN, ENCODING=PLAIN",
        "SET STORAGE GROUP TO root.sg1",
        "create aligned timeseries root.sg1.d1(s1 FLOAT encoding=RLE, s2 INT32 encoding=Gorilla compression=SNAPPY, s3 INT64, s4 BOOLEAN, s5 TEXT)",
      };

  @Before
  public void setUp() throws Exception {
    EnvironmentUtils.closeStatMonitor();
    EnvironmentUtils.envSetUp();
    enableSeqSpaceCompaction =
        IoTDBDescriptor.getInstance().getConfig().isEnableSeqSpaceCompaction();
    enableUnseqSpaceCompaction =
        IoTDBDescriptor.getInstance().getConfig().isEnableUnseqSpaceCompaction();
    enableCrossSpaceCompaction =
        IoTDBDescriptor.getInstance().getConfig().isEnableCrossSpaceCompaction();
    IoTDBDescriptor.getInstance().getConfig().setEnableSeqSpaceCompaction(false);
    IoTDBDescriptor.getInstance().getConfig().setEnableUnseqSpaceCompaction(false);
    IoTDBDescriptor.getInstance().getConfig().setEnableCrossSpaceCompaction(false);
    WriteUtil.insertData();
    EnvironmentUtils.shutdownDaemon();
    File srcDir = new File(IoTDBDescriptor.getInstance().getConfig().getDataDirs()[0]);
    FileUtils.moveDirectory(srcDir, tmpDir);
  }

  @After
  public void tearDown() throws Exception {
    IoTDBDescriptor.getInstance().getConfig().setEnableSeqSpaceCompaction(enableSeqSpaceCompaction);
    IoTDBDescriptor.getInstance()
        .getConfig()
        .setEnableUnseqSpaceCompaction(enableUnseqSpaceCompaction);
    IoTDBDescriptor.getInstance()
        .getConfig()
        .setEnableCrossSpaceCompaction(enableCrossSpaceCompaction);
    FileUtils.deleteDirectory(tmpDir);
    EnvironmentUtils.cleanEnv();
  }

  @Test
  public void test() throws Exception {
    // 1. restart IoTDB
    EnvironmentUtils.cleanEnv();
    EnvironmentUtils.envSetUp();

    // 2. test for SchemaLoader
    // TODO: use mock
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      for (String sql : schemaSqls) {
        statement.execute(sql);
      }
    } catch (Exception e) {
      Assert.fail();
      e.printStackTrace();
    }

    // 3. test for TsFileLoader
    List<File> tsFiles = getTsFilePaths(tmpDir);
    for (File tsfile : tsFiles) {
      ILoader tsFileLoader = new TsFileLoader("", tsfile);
      boolean res = tsFileLoader.load();
      System.out.println(tsfile.getAbsolutePath());
      Assert.assertTrue(res);
    }

    // 4. test for DeletionPlanLoader
    DeletionPlanLoader deletionLoader = new DeletionPlanLoader();
    deletionLoader.deleteTimeSeries(33, 38, Arrays.asList(new PartialPath("root.vehicle.**")));

    // 5. check result after loading
    // 5.1 check normal timeseries
    String sql1 = "select * from root.vehicle.*";
    String[] retArray1 =
        new String[] {
          "6,120,null,null,null",
          "9,null,123,null,null",
          "16,128,null,null,16.0",
          "18,189,198,true,18.0",
          "20,null,null,false,null",
          "29,null,null,true,1205.0",
          "99,null,1234,null,null"
        };
    String[] columnNames1 = {
      "root.vehicle.d0.s0", "root.vehicle.d0.s1", "root.vehicle.d1.s3", "root.vehicle.d1.s2"
    };
    checkResult(sql1, columnNames1, retArray1);
    // 5.2 check aligned timeseries
    String sql2 = "select * from root.sg1.d1";
    String[] retArray2 =
        new String[] {
          "1,1.0,1,null,true,aligned_test1",
          "2,2.0,2,null,null,aligned_test2",
          "3,3.0,null,null,false,aligned_test3",
          "4,4.0,4,null,true,aligned_test4",
          "5,130000.0,130000,130000,false,aligned_unseq_test1",
          "6,6.0,6,6,true,null",
          "7,7.0,7,7,false,aligned_test7",
          "8,8.0,8,8,null,aligned_test8",
          "9,9.0,9,9,false,aligned_test9",
        };
    String[] columnNames2 = {
      "root.sg1.d1.s1", "root.sg1.d1.s2", "root.sg1.d1.s3", "root.sg1.d1.s4", "root.sg1.d1.s5",
    };
    checkResult(sql2, columnNames2, retArray2);
  }

  private void checkResult(String sql, String[] columnNames, String[] retArray)
      throws ClassNotFoundException {
    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      boolean hasResultSet = statement.execute(sql);
      Assert.assertTrue(hasResultSet);
      ResultSet resultSet = statement.getResultSet();
      ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
      Map<String, Integer> map = new HashMap<>();
      for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
        map.put(resultSetMetaData.getColumnName(i), i);
      }
      assertEquals(columnNames.length + 1, resultSetMetaData.getColumnCount());
      int cnt = 0;
      while (resultSet.next()) {
        StringBuilder builder = new StringBuilder();
        builder.append(resultSet.getString(1));
        for (String columnName : columnNames) {
          int index = map.get(columnName);
          builder.append(",").append(resultSet.getString(index));
        }
        assertEquals(retArray[cnt], builder.toString());
        cnt++;
      }
      assertEquals(retArray.length, cnt);
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  /**
   * scan parentDir and return all TsFile sorted by load sequence
   *
   * @param parentDir folder to scan
   */
  private List<File> getTsFilePaths(File parentDir) {
    List<File> res = new ArrayList<>();
    if (!parentDir.exists()) {
      Assert.fail();
      return res;
    }
    scanDir(res, parentDir);
    Collections.sort(
        res,
        new Comparator<File>() {
          @Override
          public int compare(File f1, File f2) {
            int diffSg =
                f1.getParentFile()
                    .getParentFile()
                    .getParentFile()
                    .getName()
                    .compareTo(f2.getParentFile().getParentFile().getParentFile().getName());
            if (diffSg != 0) {
              return diffSg;
            } else {
              return (int)
                  (FilePathUtils.splitAndGetTsFileVersion(f1.getName())
                      - FilePathUtils.splitAndGetTsFileVersion(f2.getName()));
            }
          }
        });
    return res;
  }

  private void scanDir(List<File> tsFiles, File parentDir) {
    if (!parentDir.exists()) {
      Assert.fail();
      return;
    }
    File fa[] = parentDir.listFiles();
    for (int i = 0; i < fa.length; i++) {
      File fs = fa[i];
      if (fs.isDirectory()) {
        scanDir(tsFiles, fs);
      } else if (fs.getName().endsWith(".resource")) {
        // only add tsfile that has been flushed
        tsFiles.add(new File(fs.getAbsolutePath().substring(0, fs.getAbsolutePath().length() - 9)));
        try {
          FileUtils.delete(fs);
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    }
  }
}
