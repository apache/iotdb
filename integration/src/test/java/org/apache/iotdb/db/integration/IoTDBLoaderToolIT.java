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

import org.apache.iotdb.TsFileLoaderTool;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.StorageEngine;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
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
import java.sql.Statement;

public class IoTDBLoaderToolIT {
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
  }

  public void prepareTsFiles() throws Exception {
    try (Connection connection =
            DriverManager.getConnection("jdbc:iotdb://127.0.0.1:6667", "root", "root");
        Statement statement = connection.createStatement()) {
      statement.execute("set storage group to root.sg");
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

      for (TsFileResource resource :
          StorageEngine.getInstance()
              .getProcessor(new PartialPath("root.sg"))
              .getSequenceFileList()) {
        if (tmpDir == null) {
          tmpDir =
              resource
                      .getTsFile()
                      .getParentFile()
                      .getParentFile()
                      .getParentFile()
                      .getParentFile()
                      .getParent()
                  + File.separator
                  + "tmp";
          File tmpFile = new File(tmpDir);
          if (!tmpFile.exists()) {
            tmpFile.mkdirs();
          }
        }
        statement.execute(String.format("unload '%s' '%s'", resource.getTsFilePath(), tmpDir));
      }
      for (TsFileResource resource :
          StorageEngine.getInstance()
              .getProcessor(new PartialPath("root.sg"))
              .getUnSequenceFileList()) {
        statement.execute(String.format("unload '%s' '%s'", resource.getTsFilePath(), tmpDir));
      }
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
      TsFileLoaderTool.main(args);
      try (Connection connection =
              DriverManager.getConnection("jdbc:iotdb://127.0.0.1:6667", "root", "root");
          Statement statement = connection.createStatement()) {
        checkRes(statement.executeQuery("select * from root.sg.d1"), 3, 6);
        checkRes(statement.executeQuery("select * from root.sg.d2"), 1, 6);
        checkRes(statement.executeQuery("select * from root.sg.d3"), 1, 6);
      }
    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail();
    }
  }
}
