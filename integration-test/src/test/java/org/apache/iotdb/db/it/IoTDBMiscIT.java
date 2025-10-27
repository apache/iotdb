/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.it;

import org.apache.iotdb.db.storageengine.dataregion.flush.CompressionRatio;
import org.apache.iotdb.it.env.cluster.env.SimpleEnv;
import org.apache.iotdb.it.env.cluster.node.DataNodeWrapper;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;

import org.awaitility.Awaitility;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.File;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;

@RunWith(IoTDBTestRunner.class)
public class IoTDBMiscIT {

  @Category({LocalStandaloneIT.class})
  @Test
  public void testCompressionRatioFile() throws SQLException {
    SimpleEnv simpleEnv = new SimpleEnv();
    simpleEnv.initClusterEnvironment(1, 1);

    DataNodeWrapper nodeWrapper = simpleEnv.getDataNodeWrapper(0);
    try (Connection connection = simpleEnv.getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("insert into root.comprssion_ratio_file.d1(timestamp,s1) values(1,1.0)");
      statement.execute("flush");
      // one global file and two data region file (including one AUDIT region)
      assertEquals(3, collectCompressionRatioFiles(nodeWrapper).size());

      statement.execute("drop database root.comprssion_ratio_file");
      // one global file and system region file
      // deleting a file may not be sensed by other processes instantly
      Awaitility.await()
          .atMost(10, TimeUnit.SECONDS)
          .pollDelay(100, TimeUnit.MILLISECONDS)
          .until(() -> collectCompressionRatioFiles(nodeWrapper).size() == 2);

      statement.execute("insert into root.comprssion_ratio_file.d1(timestamp,s1) values(1,1.0)");
      statement.execute("flush");
      assertEquals(3, collectCompressionRatioFiles(nodeWrapper).size());

      statement.execute("drop database root.comprssion_ratio_file");
      Awaitility.await()
          .atMost(10, TimeUnit.SECONDS)
          .pollDelay(100, TimeUnit.MILLISECONDS)
          .until(() -> collectCompressionRatioFiles(nodeWrapper).size() == 2);

      statement.execute("insert into root.comprssion_ratio_file.d1(timestamp,s1) values(1,1.0)");
      statement.execute("flush");
      assertEquals(3, collectCompressionRatioFiles(nodeWrapper).size());

      statement.execute("insert into root.comprssion_ratio_file_2.d1(timestamp,s1) values(1,1.0)");
      statement.execute("flush");
      assertEquals(4, collectCompressionRatioFiles(nodeWrapper).size());

      statement.execute("drop database root.comprssion_ratio_file");
      Awaitility.await()
          .atMost(10, TimeUnit.SECONDS)
          .pollDelay(100, TimeUnit.MILLISECONDS)
          .until(() -> collectCompressionRatioFiles(nodeWrapper).size() == 3);

      statement.execute("drop database root.comprssion_ratio_file_2");
      Awaitility.await()
          .atMost(10, TimeUnit.SECONDS)
          .pollDelay(100, TimeUnit.MILLISECONDS)
          .until(() -> collectCompressionRatioFiles(nodeWrapper).size() == 2);
    } finally {
      simpleEnv.cleanClusterEnvironment();
    }
  }

  private List<File> collectCompressionRatioFiles(DataNodeWrapper nodeWrapper) throws SQLException {
    String compressionRatioDir =
        nodeWrapper.getSystemDir() + File.separator + CompressionRatio.COMPRESSION_RATIO_DIR;
    File compressionRatioDirFile = new File(compressionRatioDir);
    if (!compressionRatioDirFile.exists() || !compressionRatioDirFile.isDirectory()) {
      return Collections.emptyList();
    } else {
      File[] files =
          compressionRatioDirFile.listFiles(
              f -> f.getName().startsWith(CompressionRatio.FILE_PREFIX));
      return files != null ? Arrays.asList(files) : Collections.emptyList();
    }
  }
}
