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

package com.timecho.iotdb.relational.it;

import org.apache.iotdb.isession.ITableSession;
import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.env.cluster.node.DataNodeWrapper;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.TableLocalStandaloneIT;

import org.apache.tsfile.utils.Binary;
import org.awaitility.Awaitility;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

@RunWith(IoTDBTestRunner.class)
@Category({TableLocalStandaloneIT.class})
public class IoTDBObjectTieredStorageIT {

  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv()
        .getConfig()
        .getDataNodeConfig()
        .setDnDataDirs("data/datanode/data/hot;data/datanode/data/cold");
    EnvFactory.getEnv().getConfig().getDataNodeCommonConfig().setTierTTLInMs("0;-1");
    EnvFactory.getEnv().initClusterEnvironment(1, 1);
  }

  @AfterClass
  public static void tearDown() {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  /**
   * Multi-tier: CREATE/INSERT OBJECT, TTL migrates .bin to cold, READ_OBJECT still works, DELETE
   * cleans both tiers.
   */
  @Test
  public void testCreateInsertQueryDeleteAcrossTiers() throws Exception {
    try (ITableSession session = EnvFactory.getEnv().getTableSessionConnection()) {
      session.executeNonQueryStatement("CREATE DATABASE IF NOT EXISTS tier_obj");
      session.executeNonQueryStatement("USE \"tier_obj\"");
      session.executeNonQueryStatement(
          "CREATE TABLE camera(device_id STRING TAG, frame OBJECT FIELD)");
      session.executeNonQueryStatement(
          "INSERT INTO camera(time, device_id, frame) VALUES(1, 'd1', to_object(true, 0, X'cafebabe'))");
      session.executeNonQueryStatement("FLUSH");

      AtomicReference<File> coldObject = new AtomicReference<>();
      Awaitility.await()
          .atMost(90, TimeUnit.SECONDS)
          .pollInterval(1, TimeUnit.SECONDS)
          .untilAsserted(
              () -> {
                File found = findBinUnder(coldObjectRoot());
                Assert.assertNotNull("object .bin should be migrated to cold tier", found);
                coldObject.set(found);
                Assert.assertNull(
                    "source object on hot tier should be deleted after migration",
                    findBinUnder(hotObjectRoot()));
              });

      try (SessionDataSet dataSet =
          session.executeQueryStatement("SELECT READ_OBJECT(frame) FROM camera WHERE time = 1")) {
        SessionDataSet.DataIterator iterator = dataSet.iterator();
        Assert.assertTrue(iterator.next());
        Binary binary = iterator.getBlob(1);
        Assert.assertArrayEquals(
            new byte[] {(byte) 0xca, (byte) 0xfe, (byte) 0xba, (byte) 0xbe}, binary.getValues());
        Assert.assertFalse(iterator.next());
      }

      Assert.assertNotNull(coldObject.get());
      Assert.assertTrue(coldObject.get().exists());

      session.executeNonQueryStatement("DELETE FROM camera WHERE time = 1");
      Awaitility.await()
          .atMost(30, TimeUnit.SECONDS)
          .pollInterval(500, TimeUnit.MILLISECONDS)
          .untilAsserted(
              () -> {
                Assert.assertNull(findBinUnder(hotObjectRoot()));
                Assert.assertNull(findBinUnder(coldObjectRoot()));
              });
    }
  }

  /**
   * After TTL moves {@code .bin} to cold, overwriting the same timestamp installs a new hot copy
   * and drops the stale cold file. A later remigration must still serve the new payload.
   */
  @Test
  public void testOverwriteAfterColdMigration() throws Exception {
    try (ITableSession session = EnvFactory.getEnv().getTableSessionConnection()) {
      session.executeNonQueryStatement("CREATE DATABASE IF NOT EXISTS tier_obj_overwrite");
      session.executeNonQueryStatement("USE \"tier_obj_overwrite\"");
      session.executeNonQueryStatement(
          "CREATE TABLE camera(device_id STRING TAG, frame OBJECT FIELD)");
      session.executeNonQueryStatement(
          "INSERT INTO camera(time, device_id, frame) VALUES(1, 'd1', to_object(true, 0, X'cafebabe'))");
      session.executeNonQueryStatement("FLUSH");

      Awaitility.await()
          .atMost(90, TimeUnit.SECONDS)
          .pollInterval(1, TimeUnit.SECONDS)
          .untilAsserted(
              () -> {
                Assert.assertNotNull(
                    "object .bin should be migrated to cold tier", findBinUnder(coldObjectRoot()));
                Assert.assertNull(
                    "source object on hot tier should be deleted after migration",
                    findBinUnder(hotObjectRoot()));
              });

      session.executeNonQueryStatement(
          "INSERT INTO camera(time, device_id, frame) VALUES(1, 'd1', to_object(true, 0, X'deadbeef'))");
      session.executeNonQueryStatement("FLUSH");

      assertReadObject(session, new byte[] {(byte) 0xde, (byte) 0xad, (byte) 0xbe, (byte) 0xef});

      Awaitility.await()
          .atMost(90, TimeUnit.SECONDS)
          .pollInterval(1, TimeUnit.SECONDS)
          .untilAsserted(
              () ->
                  Assert.assertNull(
                      "overwritten object should remigrate off the hot tier",
                      findBinUnder(hotObjectRoot())));

      assertReadObject(session, new byte[] {(byte) 0xde, (byte) 0xad, (byte) 0xbe, (byte) 0xef});
      Assert.assertNotNull(
          "overwritten object should exist on cold after remigration",
          findBinUnder(coldObjectRoot()));

      session.executeNonQueryStatement("DELETE FROM camera WHERE time = 1");
      Awaitility.await()
          .atMost(30, TimeUnit.SECONDS)
          .pollInterval(500, TimeUnit.MILLISECONDS)
          .untilAsserted(
              () -> {
                Assert.assertNull(findBinUnder(hotObjectRoot()));
                Assert.assertNull(findBinUnder(coldObjectRoot()));
              });
    }
  }

  @Test
  public void testDropTableAfterColdMigration() throws Exception {
    try (ITableSession session = EnvFactory.getEnv().getTableSessionConnection()) {
      session.executeNonQueryStatement("CREATE DATABASE IF NOT EXISTS tier_obj_drop");
      session.executeNonQueryStatement("USE \"tier_obj_drop\"");
      session.executeNonQueryStatement(
          "CREATE TABLE camera(device_id STRING TAG, frame OBJECT FIELD)");
      session.executeNonQueryStatement(
          "INSERT INTO camera(time, device_id, frame) VALUES(1, 'd1', to_object(true, 0, X'cafebabe'))");
      session.executeNonQueryStatement("FLUSH");

      Awaitility.await()
          .atMost(90, TimeUnit.SECONDS)
          .pollInterval(1, TimeUnit.SECONDS)
          .untilAsserted(
              () -> {
                Assert.assertNotNull(
                    "object .bin should be migrated to cold tier", findBinUnder(coldObjectRoot()));
                Assert.assertNull(
                    "source object on hot tier should be deleted after migration",
                    findBinUnder(hotObjectRoot()));
              });

      session.executeNonQueryStatement("DROP TABLE camera");
      Awaitility.await()
          .atMost(30, TimeUnit.SECONDS)
          .pollInterval(500, TimeUnit.MILLISECONDS)
          .untilAsserted(
              () -> {
                Assert.assertNull(findBinUnder(hotObjectRoot()));
                Assert.assertNull(findBinUnder(coldObjectRoot()));
              });
    }
  }

  private static void assertReadObject(ITableSession session, byte[] expected) throws Exception {
    try (SessionDataSet dataSet =
        session.executeQueryStatement("SELECT READ_OBJECT(frame) FROM camera WHERE time = 1")) {
      SessionDataSet.DataIterator iterator = dataSet.iterator();
      Assert.assertTrue(iterator.next());
      Binary binary = iterator.getBlob(1);
      Assert.assertArrayEquals(expected, binary.getValues());
      Assert.assertFalse(iterator.next());
    }
  }

  private static File hotObjectRoot() {
    return objectRoot("hot");
  }

  private static File coldObjectRoot() {
    return objectRoot("cold");
  }

  private static File objectRoot(String tierName) {
    DataNodeWrapper wrapper = EnvFactory.getEnv().getDataNodeWrapperList().get(0);
    return new File(
        wrapper.getDataNodeDir()
            + File.separator
            + "data"
            + File.separator
            + tierName
            + File.separator
            + "object");
  }

  private static File findBinUnder(File root) throws Exception {
    if (!root.exists()) {
      return null;
    }
    try (Stream<Path> stream = Files.walk(root.toPath())) {
      return stream
          .filter(Files::isRegularFile)
          .filter(path -> path.getFileName().toString().endsWith(".bin"))
          .map(Path::toFile)
          .findFirst()
          .orElse(null);
    }
  }
}
