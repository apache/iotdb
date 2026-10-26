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

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;

import org.awaitility.Awaitility;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.apache.tsfile.common.constant.TsFileConstant.TSFILE_SUFFIX;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class})
public class IoTDBRecreatedTimeseriesCompactionIT {

  private static final String DATABASE = "root.repro";

  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv()
        .getConfig()
        .getCommonConfig()
        .setEnableSeqSpaceCompaction(false)
        .setEnableUnseqSpaceCompaction(false)
        .setEnableCrossSpaceCompaction(false)
        .setInnerCompactionCandidateFileNum(2);
    EnvFactory.getEnv().getConfig().getDataNodeConfig().setCompactionScheduleInterval(1000);
    EnvFactory.getEnv().initClusterEnvironment();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void testDeletionAfterRecreatingTwoLevelTimeseriesSurvivesCompaction() throws Exception {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE DATABASE " + DATABASE);
      statement.execute("CREATE TIMESERIES root.repro.s1 WITH DATATYPE=INT64");
      statement.execute("INSERT INTO root.repro(time, s1) VALUES (1, 1)");
      statement.execute("FLUSH");

      statement.execute("DELETE TIMESERIES root.repro.s1");
      statement.execute("CREATE TIMESERIES root.repro.s1 WITH DATATYPE=INT64");
      statement.execute("INSERT INTO root.repro(time, s1) VALUES (2, 2)");
      statement.execute("FLUSH");

      assertOnlyRecreatedTimeseriesDataExists(statement);
      Assert.assertEquals(2, countSequenceTsFiles());

      statement.execute("SET CONFIGURATION 'enable_seq_space_compaction'='true'");

      Awaitility.await()
          .pollInterval(200, TimeUnit.MILLISECONDS)
          .atMost(30, TimeUnit.SECONDS)
          .until(() -> countSequenceTsFiles() == 1);

      assertOnlyRecreatedTimeseriesDataExists(statement);
    }
  }

  private static void assertOnlyRecreatedTimeseriesDataExists(Statement statement)
      throws Exception {
    try (ResultSet resultSet = statement.executeQuery("SELECT s1 FROM root.repro")) {
      Assert.assertTrue(resultSet.next());
      Assert.assertEquals(2, resultSet.getLong("Time"));
      Assert.assertEquals(2, resultSet.getLong("root.repro.s1"));
      Assert.assertFalse(resultSet.next());
    }
  }

  private static long countSequenceTsFiles() throws IOException {
    Path databaseSequenceDirectory =
        Paths.get(
            EnvFactory.getEnv().getDataNodeWrapper(0).getDataPath(),
            IoTDBConstant.SEQUENCE_FOLDER_NAME,
            DATABASE);
    if (!Files.exists(databaseSequenceDirectory)) {
      return 0;
    }
    try (Stream<Path> paths = Files.walk(databaseSequenceDirectory)) {
      return paths
          .filter(Files::isRegularFile)
          .filter(path -> path.getFileName().toString().endsWith(TSFILE_SUFFIX))
          .count();
    }
  }
}
