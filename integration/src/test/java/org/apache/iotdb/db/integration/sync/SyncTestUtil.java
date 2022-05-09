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

import org.apache.iotdb.db.engine.modification.ModificationFile;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.jdbc.Config;
import org.apache.iotdb.tsfile.utils.FilePathUtils;

import org.junit.Assert;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class SyncTestUtil {

  private static final String[] sqls =
      new String[] {
        "SET STORAGE GROUP TO root.vehicle",
        "CREATE TIMESERIES root.vehicle.d0.s0 WITH DATATYPE=INT32, ENCODING=RLE",
        "CREATE TIMESERIES root.vehicle.d0.s1 WITH DATATYPE=TEXT, ENCODING=PLAIN",
        "CREATE TIMESERIES root.vehicle.d1.s2 WITH DATATYPE=FLOAT, ENCODING=RLE",
        "CREATE TIMESERIES root.vehicle.d1.s3 WITH DATATYPE=BOOLEAN, ENCODING=PLAIN",
        "SET STORAGE GROUP TO root.sg1",
        "create aligned timeseries root.sg1.d1(s1 FLOAT encoding=RLE, s2 INT32 encoding=Gorilla compression=SNAPPY, s3 INT64, s4 BOOLEAN, s5 TEXT)",
        "insert into root.sg1.d1(time, s1, s2, s3, s4, s5) aligned values(1, 1.0, 1, 1, TRUE, 'aligned_test1')",
        "insert into root.sg1.d1(time, s1, s2, s3, s5) aligned values(2, 2.0, 2, 2, 'aligned_test2')",
        "insert into root.sg1.d1(time, s1, s3, s4, s5) aligned values(3, 3.0, 3, FALSE, 'aligned_test3')",
        "insert into root.sg1.d1(time, s1, s2, s4, s5) aligned values(4, 4.0, 4, TRUE, 'aligned_test4')",
        "insert into root.sg1.d1(time, s1, s2, s4, s5) aligned values(5, 5.0, 5, TRUE, 'aligned_test5')",
        "insert into root.sg1.d1(time, s1, s2, s3, s4) aligned values(6, 6.0, 6, 6, TRUE)",
        "insert into root.sg1.d1(time, s1, s2, s3, s4, s5) aligned values(7, 7.0, 7, 7, FALSE, 'aligned_test7')",
        "insert into root.sg1.d1(time, s1, s2, s3, s5) aligned values(8, 8.0, 8, 8, 'aligned_test8')",
        "insert into root.sg1.d1(time, s1, s2, s3, s4, s5) aligned values(9, 9.0, 9, 9, FALSE, 'aligned_test9')",
        "insert into root.vehicle.d0(timestamp,s0) values(10,10)",
        "insert into root.vehicle.d0(timestamp,s0,s1) values(12,12,'12')",
        "insert into root.vehicle.d0(timestamp,s1) values(14,'14')",
        "insert into root.vehicle.d1(timestamp,s2) values(16,16.0)",
        "insert into root.vehicle.d1(timestamp,s2,s3) values(18,18.0,true)",
        "insert into root.vehicle.d1(timestamp,s3) values(20,false)",
        "flush",
        "insert into root.vehicle.d0(timestamp,s0) values(6,120)",
        "insert into root.vehicle.d0(timestamp,s0,s1) values(38,121,'122')",
        "insert into root.vehicle.d0(timestamp,s1) values(9,'123')",
        "insert into root.vehicle.d0(timestamp,s0) values(16,128)",
        "insert into root.sg1.d1(time, s1, s2, s3, s4, s5) aligned values(5, 130000.0, 130000, 130000, FALSE, 'aligned_unseq_test1')",
        "flush",
        "insert into root.vehicle.d0(timestamp,s0,s1) values(18,189,'198')",
        "insert into root.vehicle.d0(timestamp,s1) values(99,'1234')",
        "insert into root.vehicle.d1(timestamp,s2) values(14,1024.0)",
        "insert into root.vehicle.d1(timestamp,s2,s3) values(29,1205.0,true)",
        "insert into root.vehicle.d1(timestamp,s3) values(33,true)",
        "delete from root.sg1.d1.s3 where time<=3",
        "flush",
        "delete from root.vehicle.** where time >= 10 and time<=14",
        "flush",
        // no flush data
        "insert into root.sg1.d1(time, s1, s3, s4) aligned values(23, 230000.0, 230000, FALSE)",
        "insert into root.sg1.d1(time, s2, s5) aligned values(31, 31, 'aligned_test31')",
        "insert into root.sg1.d1(time, s2, s5) aligned values(32, 32, 'aligned_test32')",
        "insert into root.sg1.d1(time, s2, s5) aligned values(33, 33, 'aligned_test33')",
        "insert into root.sg1.d1(time, s2, s5) aligned values(34, 34, 'aligned_test34')",
        "insert into root.sg1.d1(time, s2, s5) aligned values(35, 35, 'aligned_test35')",
        "insert into root.sg1.d1(time, s2, s5) aligned values(36, 36, 'aligned_test36')",
        "insert into root.sg1.d1(time, s2, s5) aligned values(37, 37, 'aligned_test37')",
        "insert into root.sg1.d1(time, s2, s5) aligned values(38, 38, 'aligned_test38')",
        "insert into root.sg1.d1(time, s2, s5) aligned values(39, 39, 'aligned_test39')",
        "insert into root.sg1.d1(time, s2, s5) aligned values(40, 40, 'aligned_test40')",
      };

  public static void insertData() throws ClassNotFoundException {
    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      // create aligned and non-aligned time series
      for (String sql : sqls) {
        statement.execute(sql);
      }

    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  /**
   * make tsFile's name unique
   *
   * @param tsFiles files list, each file will be replaced by a new file with unique name
   */
  public static void renameTsFiles(List<File> tsFiles) {
    for (int i = 0; i < tsFiles.size(); i++) {
      File tsFile = tsFiles.get(i);
      File dir = tsFile.getParentFile();
      String prefix =
          dir.getParentFile().getParentFile().getName()
              + "-"
              + dir.getParentFile().getName()
              + "-"
              + dir.getName()
              + "-";
      File targetTsFile = new File(dir, prefix + tsFile.getName());
      tsFile.renameTo(targetTsFile);
      File resourceFile = new File(dir, tsFile.getName() + TsFileResource.RESOURCE_SUFFIX);
      if (resourceFile.exists()) {
        resourceFile.renameTo(new File(dir, prefix + resourceFile.getName()));
      }
      File modsFile = new File(dir, tsFile.getName() + ModificationFile.FILE_SUFFIX);
      if (modsFile.exists()) {
        modsFile.renameTo(new File(dir, prefix + modsFile.getName()));
      }
      tsFiles.set(i, targetTsFile);
    }
  }

  /**
   * scan parentDir and return all TsFile sorted by load sequence
   *
   * @param parentDir folder to scan
   */
  public static List<File> getTsFilePaths(File parentDir) {
    List<File> res = new ArrayList<>();
    if (!parentDir.exists()) {
      Assert.fail();
      return res;
    }
    scanDir(res, parentDir);
    res.sort(
        (f1, f2) -> {
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
        });
    return res;
  }

  private static void scanDir(List<File> tsFiles, File parentDir) {
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
      }
    }
  }

  public static void checkResult(
      String sql, String[] columnNames, String[] retArray, boolean hasTimeColumn)
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
      assertEquals(
          hasTimeColumn ? columnNames.length + 1 : columnNames.length,
          resultSetMetaData.getColumnCount());
      int cnt = 0;
      while (resultSet.next()) {
        StringBuilder builder = new StringBuilder();
        if (hasTimeColumn) {
          builder.append(resultSet.getString(1)).append(",");
        }
        for (String columnName : columnNames) {
          int index = map.get(columnName);
          builder.append(resultSet.getString(index)).append(",");
        }
        if (builder.length() > 0) {
          builder.deleteCharAt(builder.length() - 1);
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

  public static void checkResult(String sql, String[] columnNames, String[] retArray)
      throws ClassNotFoundException {
    checkResult(sql, columnNames, retArray, true);
  }
}
