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

import org.apache.iotdb.db.engine.StorageEngine;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.jdbc.Config;
import org.apache.iotdb.tsfile.utils.FilePathUtils;
import org.apache.iotdb.tsfile.utils.Pair;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Objects;

public class IoTDBFilePathUtilsIT {

  private static Connection connection;
  private static final Logger logger = LoggerFactory.getLogger(IoTDBFilePathUtilsIT.class);

  @BeforeClass
  public static void setUp() {
    EnvironmentUtils.closeStatMonitor();
    EnvironmentUtils.envSetUp();
  }

  private void insertData() throws ClassNotFoundException, SQLException {
    Class.forName(Config.JDBC_DRIVER_NAME);
    connection =
        DriverManager.getConnection(Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
    Statement statement = connection.createStatement();

    statement.execute("insert into root.sg1.wf01.wt01(timestamp, status) values (1000, true)");
    statement.execute("insert into root.sg1.wf01.wt01(timestamp, status) values (2000, true)");
    statement.execute("insert into root.sg1.wf01.wt01(timestamp, status) values (3000, true)");
    statement.execute("flush");
    statement.close();
  }

  @Test
  public void splitTsFilePathTest()
      throws StorageEngineException, SQLException, ClassNotFoundException {
    insertData();
    String storageGroupName = "root.sg1";
    PartialPath sgPath = null;
    try {
      sgPath = new PartialPath(storageGroupName);
    } catch (IllegalPathException e) {
      Assert.fail(e.getMessage());
    }
    Assert.assertNotNull(sgPath);
    List<TsFileResource> tsFileResources =
        StorageEngine.getInstance().getProcessor(sgPath).getSequenceFileTreeSet();
    Assert.assertNotNull(tsFileResources);

    for (TsFileResource tsFileResource : tsFileResources) {
      String sgName =
          FilePathUtils.getLogicalStorageGroupName(tsFileResource.getTsFile().getAbsolutePath());
      Assert.assertEquals(storageGroupName, sgName);

      Pair<String, Long> logicalSgNameAndTimePartitionIdPair =
          FilePathUtils.getLogicalSgNameAndTimePartitionIdPair(
              tsFileResource.getTsFile().getAbsolutePath());
      Assert.assertEquals(storageGroupName, logicalSgNameAndTimePartitionIdPair.left);
    }
  }

  private static void close() {
    if (Objects.nonNull(connection)) {
      try {
        connection.close();
      } catch (Exception e) {
        logger.error("close the connection failed,", e);
      }
    }
  }

  @AfterClass
  public static void tearDown() throws Exception {
    close();
    EnvironmentUtils.cleanEnv();
  }
}
