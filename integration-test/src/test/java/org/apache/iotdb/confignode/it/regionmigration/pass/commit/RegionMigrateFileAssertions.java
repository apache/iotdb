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

package org.apache.iotdb.confignode.it.regionmigration.pass.commit;

import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.env.cluster.node.DataNodeWrapper;

import org.awaitility.Awaitility;
import org.junit.Assert;

import java.io.File;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

final class RegionMigrateFileAssertions {

  static final String MULTI_DATA_DIRS =
      "data/datanode/data/disk0,data/datanode/data/disk1,data/datanode/data/disk2";

  private static final String SEQUENCE_FOLDER = "sequence";
  private static final String TSFILE_SUFFIX = ".tsfile";
  private static final String TSFILE_RESOURCE_SUFFIX = ".tsfile.resource";
  private static final String MODS_SUFFIX = ".mods2";

  private RegionMigrateFileAssertions() {}

  static void awaitTsFileVisibleOnReplicas(
      String database, int dataRegionId, Set<Integer> dataNodeIds) {
    awaitFileVisibleOnReplicas(database, dataRegionId, dataNodeIds, TSFILE_SUFFIX);
  }

  static void awaitTsFileResourceVisibleOnReplicas(
      String database, int dataRegionId, Set<Integer> dataNodeIds) {
    awaitFileVisibleOnReplicas(database, dataRegionId, dataNodeIds, TSFILE_RESOURCE_SUFFIX);
  }

  static void awaitTsFileResourceVisibleOnReplicas(
      Statement statement, String database, int dataRegionId, Set<Integer> dataNodeIds) {
    awaitFileVisibleOnReplicas(
        statement, database, dataRegionId, dataNodeIds, TSFILE_RESOURCE_SUFFIX);
  }

  static void awaitModsVisibleOnReplicas(
      String database, int dataRegionId, Set<Integer> dataNodeIds) {
    awaitFileVisibleOnReplicas(database, dataRegionId, dataNodeIds, MODS_SUFFIX);
  }

  static void awaitRegionReplicas(
      Statement statement, int dataRegionId, Set<Integer> expectedReplicaDataNodeIds) {
    Awaitility.await()
        .atMost(10, TimeUnit.MINUTES)
        .pollDelay(1, TimeUnit.SECONDS)
        .pollInterval(2, TimeUnit.SECONDS)
        .untilAsserted(
            () ->
                Assert.assertEquals(
                    expectedReplicaDataNodeIds, getReplicaDataNodeIds(statement, dataRegionId)));
  }

  static Set<Integer> getReplicaDataNodeIds(Statement statement, int dataRegionId)
      throws Exception {
    Set<Integer> replicaDataNodeIds = new HashSet<>();
    try (ResultSet showRegions = statement.executeQuery("SHOW REGIONS")) {
      while (showRegions.next()) {
        if ("DataRegion".equals(showRegions.getString("Type"))
            && showRegions.getInt("RegionId") == dataRegionId) {
          replicaDataNodeIds.add(showRegions.getInt("DataNodeId"));
        }
      }
    }
    Assert.assertFalse(replicaDataNodeIds.isEmpty());
    return replicaDataNodeIds;
  }

  private static void awaitFileVisibleOnReplicas(
      String database, int dataRegionId, Set<Integer> dataNodeIds, String suffix) {
    awaitFileVisibleOnReplicas(null, database, dataRegionId, dataNodeIds, suffix);
  }

  private static void awaitFileVisibleOnReplicas(
      Statement flushStatement,
      String database,
      int dataRegionId,
      Set<Integer> dataNodeIds,
      String suffix) {
    for (int dataNodeId : dataNodeIds) {
      DataNodeWrapper dataNodeWrapper =
          EnvFactory.getEnv().dataNodeIdToWrapper(dataNodeId).orElseThrow();
      Awaitility.await()
          .atMost(2, TimeUnit.MINUTES)
          .pollDelay(500, TimeUnit.MILLISECONDS)
          .pollInterval(1, TimeUnit.SECONDS)
          .untilAsserted(
              () -> {
                if (flushStatement != null) {
                  flushStatement.execute("FLUSH");
                }
                Assert.assertTrue(
                    String.format(
                        "Expected file with suffix %s for database %s region %d on DataNode %d",
                        suffix, database, dataRegionId, dataNodeId),
                    containsSequenceFileWithSuffix(
                        dataNodeWrapper, database, dataRegionId, suffix));
              });
    }
  }

  private static boolean containsSequenceFileWithSuffix(
      DataNodeWrapper dataNodeWrapper, String database, int dataRegionId, String suffix) {
    for (String dataDir : MULTI_DATA_DIRS.split(",")) {
      File regionDir =
          new File(
              dataNodeWrapper.getNodePath(),
              dataDir
                  + File.separator
                  + SEQUENCE_FOLDER
                  + File.separator
                  + database
                  + File.separator
                  + dataRegionId);
      if (containsFileWithSuffix(regionDir, suffix)) {
        return true;
      }
    }
    return false;
  }

  private static boolean containsFileWithSuffix(File file, String suffix) {
    if (!file.exists()) {
      return false;
    }
    if (file.isFile()) {
      // IoTConsensus followers can create an open zero-byte TsFile before a later FLUSH closes it.
      return file.getName().endsWith(suffix) && (TSFILE_SUFFIX.equals(suffix) || file.length() > 0);
    }
    File[] children = file.listFiles();
    if (children == null) {
      return false;
    }
    for (File child : children) {
      if (containsFileWithSuffix(child, suffix)) {
        return true;
      }
    }
    return false;
  }
}
