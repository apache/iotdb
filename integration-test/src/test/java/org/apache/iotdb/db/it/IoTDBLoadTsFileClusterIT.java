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

import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.it.utils.TsFileGenerator;
import org.apache.iotdb.itbase.category.ClusterIT;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.File;
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;

@RunWith(IoTDBTestRunner.class)
@Category({ClusterIT.class})
public class IoTDBLoadTsFileClusterIT {

  private static final long PARTITION_INTERVAL = 10_000L;
  private static final String DATABASE = "root.load_cluster";
  private static final List<String> DEVICES =
      Arrays.asList(
          DATABASE + ".d1", DATABASE + ".d2", DATABASE + ".d3", DATABASE + ".d4", DATABASE + ".d5");
  private static final List<String> MEASUREMENTS = Arrays.asList("s1", "s2", "s3", "s4");
  private static final int POINT_COUNT_PER_DEVICE = 20_000;
  private static final int REPLICATION_FACTOR = 3;
  private static final int CONFIG_NODE_NUM = 3;
  private static final int DATA_NODE_NUM = 3;

  private File tmpDir;

  @Before
  public void setUp() throws Exception {
    tmpDir = new File(Files.createTempDirectory("load-cluster-it").toUri());
    EnvFactory.getEnv().getConfig().getCommonConfig().setTimePartitionInterval(PARTITION_INTERVAL);
    EnvFactory.getEnv().getConfig().getCommonConfig().setEnforceStrongPassword(false);
    EnvFactory.getEnv().getConfig().getCommonConfig().setPipeMemoryManagementEnabled(false);
    EnvFactory.getEnv().getConfig().getCommonConfig().setDatanodeMemoryProportion("1:10:1:1:1:0");
    EnvFactory.getEnv()
        .getConfig()
        .getCommonConfig()
        .setConfigNodeConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS)
        .setSchemaRegionConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS)
        .setDataRegionConsensusProtocolClass(ConsensusFactory.IOT_CONSENSUS)
        .setSchemaReplicationFactor(REPLICATION_FACTOR)
        .setDataReplicationFactor(REPLICATION_FACTOR);
    EnvFactory.getEnv().initClusterEnvironment(CONFIG_NODE_NUM, DATA_NODE_NUM);
  }

  @After
  public void tearDown() throws Exception {
    try (final Connection connection = EnvFactory.getEnv().getConnection();
        final Statement statement = connection.createStatement()) {
      statement.execute("delete database " + DATABASE);
    } catch (final Exception ignored) {
    }

    EnvFactory.getEnv().cleanClusterEnvironment();

    final File[] files = tmpDir.listFiles();
    if (files != null) {
      for (final File file : files) {
        Assert.assertTrue(file.delete());
      }
    }
    Assert.assertTrue(tmpDir.delete());
  }

  @Test
  public void testLoadTsFileInCluster() throws Exception {
    final long writtenPointCount;
    try (final TsFileGenerator generator =
        new TsFileGenerator(new File(tmpDir, "load-cluster-1-0-0-0.tsfile"))) {
      final List<IMeasurementSchema> schemas =
          Arrays.asList(
              new MeasurementSchema(MEASUREMENTS.get(0), TSDataType.INT64, TSEncoding.PLAIN),
              new MeasurementSchema(MEASUREMENTS.get(1), TSDataType.INT64, TSEncoding.PLAIN),
              new MeasurementSchema(MEASUREMENTS.get(2), TSDataType.INT64, TSEncoding.PLAIN),
              new MeasurementSchema(MEASUREMENTS.get(3), TSDataType.INT64, TSEncoding.PLAIN));
      for (final String device : DEVICES) {
        generator.registerTimeseries(device, schemas);
        generator.generateData(device, POINT_COUNT_PER_DEVICE, 1L, false);
      }
      writtenPointCount = generator.getTotalNumber();
    }

    try (final Connection connection = EnvFactory.getEnv().getConnection();
        final Statement statement = connection.createStatement()) {
      statement.execute("create database " + DATABASE);
      for (final String device : DEVICES) {
        for (final String measurement : MEASUREMENTS) {
          statement.execute(
              "create timeseries " + device + "." + measurement + " " + TSDataType.INT64.name());
        }
      }
      statement.execute("load \"" + tmpDir.getAbsolutePath() + "\"");

      long actualPointCount = 0;
      for (final String device : DEVICES) {
        try (final ResultSet resultSet =
            statement.executeQuery(
                "select count(s1), count(s2), count(s3), count(s4) from " + device)) {
          Assert.assertTrue(resultSet.next());
          for (int columnIndex = 1; columnIndex <= MEASUREMENTS.size(); columnIndex++) {
            actualPointCount += resultSet.getLong(columnIndex);
          }
        }
      }
      Assert.assertEquals(writtenPointCount, actualPointCount);
    }
  }
}
