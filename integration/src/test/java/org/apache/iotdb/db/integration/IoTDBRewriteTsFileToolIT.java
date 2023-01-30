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

import org.apache.iotdb.RewriteTsFileTool;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.utils.FileUtils;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.jdbc.Config;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class IoTDBRewriteTsFileToolIT {
  private static final IoTDBConfig CONFIG = IoTDBDescriptor.getInstance().getConfig();

  private String tmpDir;

  @Before
  public void setup() throws Exception {
    CONFIG.setEnableCrossSpaceCompaction(false);
    CONFIG.setEnableSeqSpaceCompaction(false);
    CONFIG.setEnableUnseqSpaceCompaction(false);
    EnvironmentUtils.envSetUp();
    Class.forName(Config.JDBC_DRIVER_NAME);
  }

  @After
  public void tearDown() throws Exception {
    EnvironmentUtils.cleanEnv();
    if (tmpDir != null) {
      FileUtils.deleteDirectory(new File(tmpDir));
    }
  }

  public void unload(Statement statement)
      throws IllegalPathException, SQLException, StorageEngineException {
    //    for (TsFileResource resource :
    //        StorageEngine.getInstance()
    //            .getProcessor(new PartialPath("root.sg"))
    //            .getSequenceFileList()) {
    //      if (tmpDir == null) {
    //        tmpDir =
    //            resource
    //                    .getTsFile()
    //                    .getParentFile()
    //                    .getParentFile()
    //                    .getParentFile()
    //                    .getParentFile()
    //                    .getParent()
    //                + File.separator
    //                + "tmp";
    //        File tmpFile = new File(tmpDir);
    //        if (!tmpFile.exists()) {
    //          tmpFile.mkdirs();
    //        }
    //      }
    //      statement.execute(String.format("unload '%s' '%s'", resource.getTsFilePath(), tmpDir));
    //    }
    //    for (TsFileResource resource :
    //        StorageEngine.getInstance()
    //            .getProcessor(new PartialPath("root.sg"))
    //            .getUnSequenceFileList()) {
    //      statement.execute(String.format("unload '%s' '%s'", resource.getTsFilePath(), tmpDir));
    //    }
  }

  public void prepareTsFiles() throws Exception {
    try (Connection connection =
            DriverManager.getConnection("jdbc:iotdb://127.0.0.1:6667", "root", "root");
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE DATABASE root.sg");
      statement.execute("create timeseries root.sg.d1.s INT32");
      statement.execute("create timeseries root.sg.d2.s INT32");
      statement.execute("create timeseries root.sg.d3.s INT32");

      statement.execute("insert into root.sg.d1(timestamp, s) values(1, 1), (2, 2)");
      statement.execute("insert into root.sg.d2(timestamp, s) values(3, 3), (4, 4)");
      statement.execute("insert into root.sg.d3(timestamp, s) values(5, 5), (6, 6)");
      statement.execute("flush");

      statement.execute("insert into root.sg.d1(timestamp, s) values(3, 3), (4, 4)");
      statement.execute("insert into root.sg.d2(timestamp, s) values(1, 1), (2, 2)");
      statement.execute("insert into root.sg.d3(timestamp, s) values(1, 1), (2, 2)");
      statement.execute("flush");

      statement.execute("insert into root.sg.d1(timestamp, s) values(5, 5), (6, 6)");
      statement.execute("insert into root.sg.d2(timestamp, s) values(5, 5), (6, 6)");
      statement.execute("insert into root.sg.d3(timestamp, s) values(3, 3), (4, 4)");
      statement.execute("flush");

      statement.execute("delete from root.sg.d1.s where timestamp <= 2");

      statement.execute("create aligned timeseries root.sg.a(s1 INT32, s2 TEXT)");
      statement.execute("insert into root.sg.a(time, s1, s2) aligned values(1, 1, '1')");
      statement.execute("insert into root.sg.a(time, s1, s2) aligned values(2, 2, '2')");
      statement.execute("insert into root.sg.a(time, s2) aligned values(3, '3')");
      statement.execute("insert into root.sg.a(time, s1, s2) aligned values(4, 4, '3')");
      statement.execute("flush");

      statement.execute("delete from root.sg.a.s1 where time > 2");

      unload(statement);
    }
  }

  private void checkRes(ResultSet resultSet, int start, int end) throws Exception {
    while (resultSet.next()) {
      Assert.assertEquals(start, resultSet.getInt(2));
      start += 1;
    }
    Assert.assertEquals(end, start - 1);
  }

  @Test
  public void testLoadTsFile() {
    try {
      prepareTsFiles();

      String[] args = {"-h", "127.0.0.1", "-p", "6667", "-u", "root", "-pw", "root", "-f", tmpDir};
      RewriteTsFileTool.main(args);
      try (Connection connection =
              DriverManager.getConnection("jdbc:iotdb://127.0.0.1:6667", "root", "root");
          Statement statement = connection.createStatement()) {
        checkRes(statement.executeQuery("select * from root.sg.d1"), 3, 6);
        checkRes(statement.executeQuery("select * from root.sg.d2"), 1, 6);
        checkRes(statement.executeQuery("select * from root.sg.d3"), 1, 6);
        checkRes(statement.executeQuery("select s1 from root.sg.a"), 1, 2);
      }
    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail();
    }
  }

  @Test
  public void testAlignedTsFileR() throws Exception {
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      for (int fileIndex = 0; fileIndex < 5; fileIndex++) {
        for (long timestamp = fileIndex * 512, end = fileIndex * 512 + 512;
            timestamp < end;
            ++timestamp) {
          for (int deviceIndex = 0; deviceIndex < 5; ++deviceIndex) {
            statement.execute(
                String.format(
                    "insert into root.sg.d%d(time, s1, s2, s3) aligned values(%d, %d, %d, %d)",
                    deviceIndex, timestamp, timestamp + 1, timestamp + 2, timestamp + 3));
          }
        }
        statement.execute("FLUSH");
      }
      unload(statement);
    }
    RewriteTsFileTool.main(
        new String[] {
          "-h",
          "127.0.0.1",
          "-p",
          "6667",
          "-u",
          "root",
          "-pw",
          "root",
          "-f",
          tmpDir,
          "-rm",
          "r",
          "-ig"
        });
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      for (long timestamp = 0; timestamp < 512 * 5; ++timestamp) {
        for (int deviceIndex = 0; deviceIndex < 5; ++deviceIndex) {
          ResultSet resultSet =
              statement.executeQuery(
                  String.format(
                      "select s1, s2, s3 from root.sg.d%d where time=%d", deviceIndex, timestamp));
          Assert.assertTrue(resultSet.next());
          float s1Val = resultSet.getFloat("root.sg.d" + deviceIndex + ".s1");
          float s2Val = resultSet.getFloat("root.sg.d" + deviceIndex + ".s2");
          float s3Val = resultSet.getFloat("root.sg.d" + deviceIndex + ".s3");
          Assert.assertEquals(s1Val, timestamp + 1, 0.001);
          Assert.assertEquals(s2Val, timestamp + 2, 0.001);
          Assert.assertEquals(s3Val, timestamp + 3, 0.001);
          Assert.assertFalse(resultSet.next());
          resultSet.close();
        }
      }
    }
  }

  @Test
  public void testAlignedTsFileS() throws Exception {
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      for (int fileIndex = 0; fileIndex < 5; fileIndex++) {
        for (long timestamp = fileIndex * 512, end = fileIndex * 512 + 512;
            timestamp < end;
            ++timestamp) {
          for (int deviceIndex = 0; deviceIndex < 5; ++deviceIndex) {
            statement.execute(
                String.format(
                    "insert into root.sg.d%d(time, s1, s2, s3) aligned values(%d, %d, %d, %d)",
                    deviceIndex, timestamp, timestamp + 1, timestamp + 2, timestamp + 3));
          }
        }
        statement.execute("FLUSH");
      }
      unload(statement);
    }
    RewriteTsFileTool.main(
        new String[] {
          "-h",
          "127.0.0.1",
          "-p",
          "6667",
          "-u",
          "root",
          "-pw",
          "root",
          "-f",
          tmpDir,
          "-rm",
          "s",
          "-ig"
        });
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      for (long timestamp = 0; timestamp < 512 * 5; ++timestamp) {
        for (int deviceIndex = 0; deviceIndex < 5; ++deviceIndex) {
          ResultSet resultSet =
              statement.executeQuery(
                  String.format(
                      "select s1, s2, s3 from root.sg.d%d where time=%d", deviceIndex, timestamp));
          Assert.assertTrue(resultSet.next());
          float s1Val = resultSet.getFloat("root.sg.d" + deviceIndex + ".s1");
          float s2Val = resultSet.getFloat("root.sg.d" + deviceIndex + ".s2");
          float s3Val = resultSet.getFloat("root.sg.d" + deviceIndex + ".s3");
          Assert.assertEquals(s1Val, timestamp + 1, 0.001);
          Assert.assertEquals(s2Val, timestamp + 2, 0.001);
          Assert.assertEquals(s3Val, timestamp + 3, 0.001);
          Assert.assertFalse(resultSet.next());
          resultSet.close();
        }
      }
    }
  }

  @Test
  public void testAlignedTsFileWithNullR() throws Exception {
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      for (int fileIndex = 0; fileIndex < 5; fileIndex++) {
        for (long timestamp = fileIndex * 512, end = fileIndex * 512 + 512;
            timestamp < end;
            ++timestamp) {
          for (int deviceIndex = 0; deviceIndex < 5; ++deviceIndex) {
            if (timestamp % 3 == 0) {
              statement.execute(
                  String.format(
                      "insert into root.sg.d%d(time, s2, s3) aligned values(%d, %d, %d)",
                      deviceIndex, timestamp, timestamp + 2, timestamp + 3));
            } else if (timestamp % 3 == 1) {
              statement.execute(
                  String.format(
                      "insert into root.sg.d%d(time, s1, s3) aligned values(%d, %d, %d)",
                      deviceIndex, timestamp, timestamp + 1, timestamp + 3));
            } else {
              statement.execute(
                  String.format(
                      "insert into root.sg.d%d(time, s1, s2) aligned values(%d, %d, %d)",
                      deviceIndex, timestamp, timestamp + 1, timestamp + 2));
            }
          }
        }
        statement.execute("FLUSH");
      }
      unload(statement);
    }
    RewriteTsFileTool.main(
        new String[] {
          "-h",
          "127.0.0.1",
          "-p",
          "6667",
          "-u",
          "root",
          "-pw",
          "root",
          "-f",
          tmpDir,
          "-rm",
          "r",
          "-ig"
        });
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      for (long timestamp = 0; timestamp < 512 * 5; ++timestamp) {
        for (int deviceIndex = 0; deviceIndex < 5; ++deviceIndex) {
          ResultSet resultSet =
              statement.executeQuery(
                  String.format(
                      "select s1, s2, s3 from root.sg.d%d where time=%d", deviceIndex, timestamp));
          Assert.assertTrue(resultSet.next());
          float s1Val = resultSet.getFloat("root.sg.d" + deviceIndex + ".s1");
          float s2Val = resultSet.getFloat("root.sg.d" + deviceIndex + ".s2");
          float s3Val = resultSet.getFloat("root.sg.d" + deviceIndex + ".s3");
          if (timestamp % 3 != 0) {
            Assert.assertEquals(s1Val, timestamp + 1, 0.001);
          } else {
            Assert.assertEquals(s1Val, 0, 0.001);
          }
          if (timestamp % 3 != 1) {
            Assert.assertEquals(s2Val, timestamp + 2, 0.001);
          } else {
            Assert.assertEquals(s2Val, 0, 0.001);
          }
          if (timestamp % 3 != 2) {
            Assert.assertEquals(s3Val, timestamp + 3, 0.001);
          } else {
            Assert.assertEquals(s3Val, 0, 0.001);
          }
          Assert.assertFalse(resultSet.next());
          resultSet.close();
        }
      }
    }
  }

  @Test
  public void testAlignedTsFileWithNullS() throws Exception {
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      for (int fileIndex = 0; fileIndex < 5; fileIndex++) {
        for (long timestamp = fileIndex * 512, end = fileIndex * 512 + 512;
            timestamp < end;
            ++timestamp) {
          for (int deviceIndex = 0; deviceIndex < 5; ++deviceIndex) {
            if (timestamp % 3 == 0) {
              statement.execute(
                  String.format(
                      "insert into root.sg.d%d(time, s2, s3) aligned values(%d, %d, %d)",
                      deviceIndex, timestamp, timestamp + 2, timestamp + 3));
            } else if (timestamp % 3 == 1) {
              statement.execute(
                  String.format(
                      "insert into root.sg.d%d(time, s1, s3) aligned values(%d, %d, %d)",
                      deviceIndex, timestamp, timestamp + 1, timestamp + 3));
            } else {
              statement.execute(
                  String.format(
                      "insert into root.sg.d%d(time, s1, s2) aligned values(%d, %d, %d)",
                      deviceIndex, timestamp, timestamp + 1, timestamp + 2));
            }
          }
        }
        statement.execute("FLUSH");
      }
      unload(statement);
    }
    RewriteTsFileTool.main(
        new String[] {
          "-h",
          "127.0.0.1",
          "-p",
          "6667",
          "-u",
          "root",
          "-pw",
          "root",
          "-f",
          tmpDir,
          "-rm",
          "s",
          "-ig"
        });
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      for (long timestamp = 0; timestamp < 512 * 5; ++timestamp) {
        for (int deviceIndex = 0; deviceIndex < 5; ++deviceIndex) {
          ResultSet resultSet =
              statement.executeQuery(
                  String.format(
                      "select s1, s2, s3 from root.sg.d%d where time=%d", deviceIndex, timestamp));
          Assert.assertTrue(resultSet.next());
          float s1Val = resultSet.getFloat("root.sg.d" + deviceIndex + ".s1");
          float s2Val = resultSet.getFloat("root.sg.d" + deviceIndex + ".s2");
          float s3Val = resultSet.getFloat("root.sg.d" + deviceIndex + ".s3");
          if (timestamp % 3 != 0) {
            Assert.assertEquals(s1Val, timestamp + 1, 0.001);
          } else {
            Assert.assertEquals(s1Val, 0, 0.001);
          }
          if (timestamp % 3 != 1) {
            Assert.assertEquals(s2Val, timestamp + 2, 0.001);
          } else {
            Assert.assertEquals(s2Val, 0, 0.001);
          }
          if (timestamp % 3 != 2) {
            Assert.assertEquals(s3Val, timestamp + 3, 0.001);
          } else {
            Assert.assertEquals(s3Val, 0, 0.001);
          }
          Assert.assertFalse(resultSet.next());
          resultSet.close();
        }
      }
    }
  }

  @Test
  public void testSimpleTsFileWithDeletionR() throws Exception {
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      for (int fileIndex = 0; fileIndex < 5; fileIndex++) {
        for (long timestamp = fileIndex * 512, end = fileIndex * 512 + 512;
            timestamp < end;
            ++timestamp) {
          for (int deviceIndex = 0; deviceIndex < 5; ++deviceIndex) {
            statement.execute(
                String.format(
                    "insert into root.sg.d%d(time, s1, s2, s3) aligned values(%d, %d, %d, %d)",
                    deviceIndex, timestamp, timestamp + 1, timestamp + 2, timestamp + 3));
          }
        }
        statement.execute("FLUSH");
      }
    }
    long deleteStart = 1024, deleteEnd = 2048;
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      for (int deviceIndex = 0; deviceIndex < 5; ++deviceIndex) {
        statement.execute(
            "delete from root.sg.d"
                + deviceIndex
                + ".** where time >= "
                + deleteStart
                + " and time <= "
                + deleteEnd);
      }
      unload(statement);
    }
    RewriteTsFileTool.main(
        new String[] {
          "-h",
          "127.0.0.1",
          "-p",
          "6667",
          "-u",
          "root",
          "-pw",
          "root",
          "-f",
          tmpDir,
          "-rm",
          "r",
          "-ig"
        });
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      for (long timestamp = 0; timestamp < 512 * 5; ++timestamp) {
        for (int deviceIndex = 0; deviceIndex < 5; ++deviceIndex) {
          ResultSet resultSet =
              statement.executeQuery(
                  String.format(
                      "select s1, s2, s3 from root.sg.d%d where time=%d", deviceIndex, timestamp));
          if (timestamp >= deleteStart && timestamp <= deleteEnd) {
            Assert.assertFalse(resultSet.next());
            continue;
          }
          Assert.assertTrue(resultSet.next());
          float s1Val = resultSet.getFloat("root.sg.d" + deviceIndex + ".s1");
          float s2Val = resultSet.getFloat("root.sg.d" + deviceIndex + ".s2");
          float s3Val = resultSet.getFloat("root.sg.d" + deviceIndex + ".s3");
          Assert.assertEquals(s1Val, timestamp + 1, 0.001);
          Assert.assertEquals(s2Val, timestamp + 2, 0.001);
          Assert.assertEquals(s3Val, timestamp + 3, 0.001);
          Assert.assertFalse(resultSet.next());
          resultSet.close();
        }
      }
    }
  }

  @Test
  public void testSimpleTsFileWithDeletionS() throws Exception {
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      for (int fileIndex = 0; fileIndex < 5; fileIndex++) {
        for (long timestamp = fileIndex * 512, end = fileIndex * 512 + 512;
            timestamp < end;
            ++timestamp) {
          for (int deviceIndex = 0; deviceIndex < 5; ++deviceIndex) {
            statement.execute(
                String.format(
                    "insert into root.sg.d%d(time, s1, s2, s3) aligned values(%d, %d, %d, %d)",
                    deviceIndex, timestamp, timestamp + 1, timestamp + 2, timestamp + 3));
          }
        }
        statement.execute("FLUSH");
      }
    }
    long deleteStart = 1024, deleteEnd = 2048;
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      for (int deviceIndex = 0; deviceIndex < 5; ++deviceIndex) {
        statement.execute(
            "delete from root.sg.d"
                + deviceIndex
                + ".** where time >= "
                + deleteStart
                + " and time <= "
                + deleteEnd);
      }
      unload(statement);
    }
    RewriteTsFileTool.main(
        new String[] {
          "-h",
          "127.0.0.1",
          "-p",
          "6667",
          "-u",
          "root",
          "-pw",
          "root",
          "-f",
          tmpDir,
          "-rm",
          "s",
          "-ig"
        });
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      for (long timestamp = 0; timestamp < 512 * 5; ++timestamp) {
        for (int deviceIndex = 0; deviceIndex < 5; ++deviceIndex) {
          ResultSet resultSet =
              statement.executeQuery(
                  String.format(
                      "select s1, s2, s3 from root.sg.d%d where time=%d", deviceIndex, timestamp));
          if (timestamp >= deleteStart && timestamp <= deleteEnd) {
            Assert.assertFalse(resultSet.next());
            continue;
          }
          Assert.assertTrue(resultSet.next());
          float s1Val = resultSet.getFloat("root.sg.d" + deviceIndex + ".s1");
          float s2Val = resultSet.getFloat("root.sg.d" + deviceIndex + ".s2");
          float s3Val = resultSet.getFloat("root.sg.d" + deviceIndex + ".s3");
          Assert.assertEquals(s1Val, timestamp + 1, 0.001);
          Assert.assertEquals(s2Val, timestamp + 2, 0.001);
          Assert.assertEquals(s3Val, timestamp + 3, 0.001);
          Assert.assertFalse(resultSet.next());
          resultSet.close();
        }
      }
    }
  }

  @Test
  public void testWriteAlignedTsFileWithDeletion() {}
}
