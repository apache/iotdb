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

import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.isession.SessionConfig;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.env.cluster.node.DataNodeWrapper;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.TableClusterIT;
import org.apache.iotdb.itbase.category.TableLocalStandaloneIT;
import org.apache.iotdb.itbase.env.BaseEnv;
import org.apache.iotdb.itbase.env.DataNodeConfig;

import org.awaitility.Awaitility;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.DeleteBucketRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;

import java.io.File;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@RunWith(IoTDBTestRunner.class)
@Category({TableLocalStandaloneIT.class, TableClusterIT.class})
public class IoTDBObjectStorageMinIOIT {

  private static final String MINIO_ENDPOINT =
      System.getProperty("iotdb.it.minio.endpoint", "http://127.0.0.1:9000");
  private static final String ACCESS_KEY =
      System.getProperty("iotdb.it.minio.accessKey", "minioadmin");
  private static final String SECRET_KEY =
      System.getProperty(
          "iotdb.it.minio.secretKey", "fce2e70eb69fab571024cf9a5526ebd700594e99fb3cc902");
  private static final String BUCKET = "iotdb-minio-it-" + UUID.randomUUID();
  private static final String REGION = "us-east-1";

  private static S3Client s3Client;
  private static boolean bucketCreated;
  private static boolean environmentStarted;

  @BeforeClass
  public static void setUp() throws Exception {
    s3Client = createS3Client();
    try {
      s3Client.listBuckets();
    } catch (RuntimeException e) {
      Assume.assumeNoException("MinIO is unavailable at " + MINIO_ENDPOINT, e);
    }
    s3Client.createBucket(CreateBucketRequest.builder().bucket(BUCKET).build());
    bucketCreated = true;

    DataNodeConfig dataNodeConfig = EnvFactory.getEnv().getConfig().getDataNodeConfig();
    dataNodeConfig
        .setDnDataDirs("data/datanode/data;OBJECT_STORAGE")
        .setObjectStorageType("AWS_S3")
        .setObjectStorageEndpoint(MINIO_ENDPOINT)
        .setObjectStorageRegion(REGION)
        .setObjectStorageBucket(BUCKET)
        .setObjectStorageAccessKey(ACCESS_KEY)
        .setObjectStorageAccessSecret(SECRET_KEY)
        .setEnablePathStyleAccess(true)
        .setCompactionScheduleInterval(1000);
    EnvFactory.getEnv()
        .getConfig()
        .getCommonConfig()
        .setDataReplicationFactor(3)
        .setSchemaReplicationFactor(1)
        .setDataRegionConsensusProtocolClass(ConsensusFactory.IOT_CONSENSUS)
        .setTimePartitionInterval(1000);
    EnvFactory.getEnv().getConfig().getDataNodeCommonConfig().setTierTTLInMs("0;-1");
    EnvFactory.getEnv().initClusterEnvironment(1, 3);
    environmentStarted = true;
  }

  @AfterClass
  public static void tearDown() throws Exception {
    try {
      if (environmentStarted) {
        EnvFactory.getEnv().cleanClusterEnvironment();
      }
    } finally {
      if (s3Client != null) {
        try {
          if (bucketCreated) {
            deleteBucket();
          }
        } finally {
          s3Client.close();
        }
      }
    }
  }

  /**
   * OBJECT {@code .bin} is written to the local tier, migrated to MinIO by tier TTL, then {@code
   * READ_OBJECT} still returns the payload.
   */
  @Test
  public void testObjectFileMigratedToMinIO() throws Exception {
    try (Connection connection = EnvFactory.getEnv().getTableConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE DATABASE object_storage_obj_db");
      statement.execute("USE object_storage_obj_db");
      statement.execute("CREATE TABLE camera(device STRING TAG, frame OBJECT FIELD)");
      statement.execute(
          "INSERT INTO camera(time, device, frame) VALUES (1, 'd1', to_object(true, 0, X'cafebabe'))");
      statement.execute("FLUSH");

      awaitObjectMigratedToMinIO();

      try (ResultSet resultSet =
          statement.executeQuery("SELECT READ_OBJECT(frame) FROM camera WHERE time = 1")) {
        Assert.assertTrue(resultSet.next());
        Assert.assertEquals("0xcafebabe", resultSet.getString(1));
        Assert.assertFalse(resultSet.next());
      }
    }
  }

  /**
   * After TTL moves {@code .bin} to MinIO: full {@code READ_OBJECT} (builtin scalar / UDF path that
   * pulls the entire file) and offset/length range reads must still return the correct bytes via
   * OBJECT_STORAGE ({@code OSFileChannel} Range GET).
   */
  @Test
  public void testReadObjectFullAndOffsetAfterMinIOMigration() throws Exception {
    try (Connection connection = EnvFactory.getEnv().getTableConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE DATABASE object_storage_obj_read_db");
      statement.execute("USE object_storage_obj_read_db");
      statement.execute("CREATE TABLE camera(device STRING TAG, frame OBJECT FIELD)");
      // 0xca fe ba be — used for offset/length slicing assertions below
      statement.execute(
          "INSERT INTO camera(time, device, frame) VALUES (1, 'd1', to_object(true, 0, X'cafebabe'))");
      statement.execute("FLUSH");

      awaitObjectMigratedToMinIO();

      // Full file via builtin READ_OBJECT (same ObjectTypeUtils / UDF Record.readObject path).
      try (ResultSet resultSet =
          statement.executeQuery("SELECT READ_OBJECT(frame) FROM camera WHERE time = 1")) {
        Assert.assertTrue(resultSet.next());
        Assert.assertEquals("0xcafebabe", resultSet.getString(1));
        Assert.assertFalse(resultSet.next());
      }

      // Explicit full-file form used by UDF-style APIs: offset=0, length=-1 (to EOF).
      try (ResultSet resultSet =
          statement.executeQuery("SELECT READ_OBJECT(frame, 0, -1) FROM camera WHERE time = 1")) {
        Assert.assertTrue(resultSet.next());
        Assert.assertEquals("0xcafebabe", resultSet.getString(1));
        Assert.assertFalse(resultSet.next());
      }

      // Mid-file range: bytes[1..2] of cafebabe → fe ba
      try (ResultSet resultSet =
          statement.executeQuery("SELECT READ_OBJECT(frame, 1, 2) FROM camera WHERE time = 1")) {
        Assert.assertTrue(resultSet.next());
        Assert.assertEquals("0xfeba", resultSet.getString(1));
        Assert.assertFalse(resultSet.next());
      }

      // Offset-only (to EOF): bytes[2..] → ba be
      try (ResultSet resultSet =
          statement.executeQuery("SELECT READ_OBJECT(frame, 2) FROM camera WHERE time = 1")) {
        Assert.assertTrue(resultSet.next());
        Assert.assertEquals("0xbabe", resultSet.getString(1));
        Assert.assertFalse(resultSet.next());
      }
    }
  }

  /**
   * After TTL moves {@code .bin} to MinIO, overwriting the same timestamp must keep serving the new
   * payload after the hot copy remigrates.
   */
  @Test
  public void testOverwriteAfterMinIOMigration() throws Exception {
    try (Connection connection = EnvFactory.getEnv().getTableConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE DATABASE object_storage_obj_overwrite_db");
      statement.execute("USE object_storage_obj_overwrite_db");
      statement.execute("CREATE TABLE camera(device STRING TAG, frame OBJECT FIELD)");
      Set<String> binsBeforeThisTest = new HashSet<>(listRemoteBinKeys());
      statement.execute(
          "INSERT INTO camera(time, device, frame) VALUES (1, 'd1', to_object(true, 0, X'cafebabe'))");
      statement.execute("FLUSH");

      awaitObjectMigratedToMinIO();

      statement.execute(
          "INSERT INTO camera(time, device, frame) VALUES (1, 'd1', to_object(true, 0, X'deadbeef'))");
      statement.execute("FLUSH");

      try (ResultSet resultSet =
          statement.executeQuery("SELECT READ_OBJECT(frame) FROM camera WHERE time = 1")) {
        Assert.assertTrue(resultSet.next());
        Assert.assertEquals("0xdeadbeef", resultSet.getString(1));
        Assert.assertFalse(resultSet.next());
      }

      Awaitility.await()
          .atMost(90, TimeUnit.SECONDS)
          .pollInterval(1, TimeUnit.SECONDS)
          .untilAsserted(
              () ->
                  Assert.assertNull(
                      "overwritten object should remigrate off the local tier", findLocalBin()));

      try (ResultSet resultSet =
          statement.executeQuery("SELECT READ_OBJECT(frame) FROM camera WHERE time = 1")) {
        Assert.assertTrue(resultSet.next());
        Assert.assertEquals("0xdeadbeef", resultSet.getString(1));
        Assert.assertFalse(resultSet.next());
      }
    }
  }

  /**
   * After TTL moves {@code .bin} to MinIO, {@code DELETE FROM} must unlink the remote object and
   * {@code READ_OBJECT} must return no rows.
   */
  @Test
  public void testDeleteAfterMinIOMigration() throws Exception {
    try (Connection connection = EnvFactory.getEnv().getTableConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE DATABASE object_storage_obj_delete_db");
      statement.execute("USE object_storage_obj_delete_db");
      statement.execute("CREATE TABLE camera(device STRING TAG, frame OBJECT FIELD)");
      statement.execute(
          "INSERT INTO camera(time, device, frame) VALUES (1, 'd1', to_object(true, 0, X'cafebabe'))");
      statement.execute("FLUSH");

      Set<String> migrated = awaitObjectMigratedToMinIOAndKeys();

      statement.execute("DELETE FROM camera WHERE time = 1");
      Awaitility.await()
          .atMost(60, TimeUnit.SECONDS)
          .pollInterval(500, TimeUnit.MILLISECONDS)
          .untilAsserted(
              () -> {
                Set<String> remaining = new HashSet<>(listRemoteBinKeys());
                for (String key : migrated) {
                  Assert.assertFalse(
                      "deleted object should leave MinIO: " + key, remaining.contains(key));
                }
                Assert.assertNull(findLocalBin());
              });

      try (ResultSet resultSet =
          statement.executeQuery("SELECT READ_OBJECT(frame) FROM camera WHERE time = 1")) {
        Assert.assertFalse(resultSet.next());
      }
    }
  }

  /**
   * After TTL moves {@code .bin} to MinIO, {@code DROP TABLE} must prefix-delete the remote table
   * directory.
   */
  @Test
  public void testDropTableAfterMinIOMigration() throws Exception {
    try (Connection connection = EnvFactory.getEnv().getTableConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE DATABASE object_storage_obj_drop_db");
      statement.execute("USE object_storage_obj_drop_db");
      statement.execute("CREATE TABLE camera(device STRING TAG, frame OBJECT FIELD)");
      statement.execute(
          "INSERT INTO camera(time, device, frame) VALUES (1, 'd1', to_object(true, 0, X'cafebabe'))");
      statement.execute("FLUSH");

      Set<String> migrated = awaitObjectMigratedToMinIOAndKeys();

      statement.execute("DROP TABLE camera");
      Awaitility.await()
          .atMost(120, TimeUnit.SECONDS)
          .pollInterval(500, TimeUnit.MILLISECONDS)
          .untilAsserted(
              () -> {
                Set<String> remaining = new HashSet<>(listRemoteBinKeys());
                for (String key : migrated) {
                  Assert.assertFalse(
                      "DROP TABLE should remove MinIO .bin: " + key, remaining.contains(key));
                }
                Assert.assertNull(findLocalBin());
              });
    }
  }

  /**
   * OBJECT last-tier single-replica (SharedStorageCompaction) on a 1C3D IoTConsensus cluster
   * ({@code dataReplicationFactor=3}, last data dir {@code OBJECT_STORAGE}, {@code
   * timePartitionInterval=1000}).
   *
   * <p>Writes three rows into time partition 0 (time=1/500) and waits until their {@code .bin}
   * files have TTL-migrated to MinIO as multi-replica. Then writes time=2000 so partition 0 is no
   * longer the latest partition and SharedStorageCompaction can run: each sealed-partition OBJECT
   * file is reduced to one last-tier replica (counted by MinIO key prefix / DataNode id of that
   * object's relative path), while the latest partition stays multi-replica.
   *
   * <p>After share, followers no longer have a last-tier {@code .bin}. Pin {@code READ_OBJECT} to
   * every live DataNode: the leader still reads its own last-tier object; followers remap the
   * leader OS path via the TsFile {@code RemoteStorageBlock}. Then overwrite the same timestamp,
   * DELETE one device, and DROP TABLE — this test's leftover MinIO {@code .bin} must be gone.
   */
  @Test
  public void testObjectLastTierSingleReplica() throws Exception {
    final String database = "object_storage_share_obj_db";
    final Set<String> keysBefore = new HashSet<>(listAllKeys());
    try (Connection connection = EnvFactory.getEnv().getTableConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE DATABASE " + database);
      statement.execute("USE " + database);
      statement.execute("CREATE TABLE camera(device STRING TAG, frame OBJECT FIELD)");

      statement.execute(
          "INSERT INTO camera(time, device, frame) VALUES "
              + "(1, 'd1', to_object(true, 0, X'cafebabe')), "
              + "(1, 'd2', to_object(true, 0, X'deadbeef')), "
              + "(500, 'd1', to_object(true, 0, X'11111111'))");
      statement.execute("FLUSH");

      awaitObjectMigratedToMinIO();
      awaitPartitionObjectReplicas(statement, keysBefore, 0, 2, false);

      // Open a newer time partition so partition 0 becomes eligible for SharedStorageCompaction.
      statement.execute(
          "INSERT INTO camera(time, device, frame) VALUES (2000, 'd1', to_object(true, 0, X'22222222'))");
      statement.execute("FLUSH");

      awaitPartitionObjectReplicas(statement, keysBefore, 0, 1, true);
      Awaitility.await()
          .atMost(90, TimeUnit.SECONDS)
          .pollInterval(2, TimeUnit.SECONDS)
          .untilAsserted(
              () -> {
                statement.execute("FLUSH");
                int prefixes =
                    countReplicaPrefixes(
                        createdSince(keysBefore, key -> isObjectBinInTimePartition(key, 2)));
                Assert.assertTrue(
                    "latest partition OBJECT .bin should still be multi-replica, prefixes="
                        + prefixes,
                    prefixes >= 2);
              });

      // After share, pin READ_OBJECT to every live DataNode. The leader still reads its own
      // last-tier .bin; followers have no local copy and must remap via RemoteStorageBlock.
      Awaitility.await()
          .atMost(90, TimeUnit.SECONDS)
          .pollInterval(1, TimeUnit.SECONDS)
          .untilAsserted(
              () -> {
                assertPayloadsOnEveryReplica(database, "d1", 1, "0xcafebabe");
                assertPayloadsOnEveryReplica(database, "d2", 1, "0xdeadbeef");
                assertPayloadsOnEveryReplica(database, "d1", 500, "0x11111111");
                assertPayloadsOnEveryReplica(database, "d1", 2000, "0x22222222");
                assertOffsetReadOnEveryReplica(database, "d1", 1, 1, 2, "0xfeba");
                assertRowsEverywhere(
                    database,
                    new String[][] {
                      {"1", "d1"},
                      {"1", "d2"},
                      {"500", "d1"},
                      {"2000", "d1"},
                    });
              });

      statement.execute(
          "INSERT INTO camera(time, device, frame) VALUES (1, 'd1', to_object(true, 0, X'aabbccdd'))");
      statement.execute("FLUSH");
      Awaitility.await()
          .atMost(90, TimeUnit.SECONDS)
          .pollInterval(1, TimeUnit.SECONDS)
          .untilAsserted(
              () -> {
                assertPayloadsOnEveryReplica(database, "d1", 1, "0xaabbccdd");
                assertPayloadsOnEveryReplica(database, "d2", 1, "0xdeadbeef");
              });

      statement.execute("DELETE FROM camera WHERE time = 1 AND device = 'd2'");
      Awaitility.await()
          .atMost(60, TimeUnit.SECONDS)
          .pollInterval(500, TimeUnit.MILLISECONDS)
          .untilAsserted(
              () -> {
                try (ResultSet resultSet =
                    statement.executeQuery(
                        "SELECT device FROM camera WHERE time = 1 AND device = 'd2'")) {
                  Assert.assertFalse(resultSet.next());
                }
                assertPayloadsOnEveryReplica(database, "d1", 1, "0xaabbccdd");
                assertPayloadsOnEveryReplica(database, "d1", 500, "0x11111111");
                assertPayloadsOnEveryReplica(database, "d1", 2000, "0x22222222");
                assertRowsEverywhere(
                    database,
                    new String[][] {
                      {"1", "d1"},
                      {"500", "d1"},
                      {"2000", "d1"},
                    });
              });

      statement.execute("DROP TABLE camera");
      Awaitility.await()
          .atMost(90, TimeUnit.SECONDS)
          .pollInterval(1, TimeUnit.SECONDS)
          .untilAsserted(
              () -> {
                Set<String> leftover = new HashSet<>(listRemoteBinKeys());
                leftover.removeAll(keysBefore);
                Assert.assertTrue(
                    "DROP TABLE after share should remove this test's MinIO .bin: " + leftover,
                    leftover.isEmpty());
              });
    }
  }

  @Test
  public void testIoTDBReadsTsFileMigratedToMinIO() throws Exception {
    try (Connection connection = EnvFactory.getEnv().getTableConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE DATABASE object_storage_db");
      statement.execute("USE object_storage_db");
      statement.execute("CREATE TABLE sensor(device STRING TAG, value INT32 FIELD)");
      statement.execute(
          "INSERT INTO sensor(time, device, value) VALUES (1, 'd1', 11), (2, 'd1', 22), (3, 'd1', 33)");
      statement.execute("FLUSH");

      Awaitility.await()
          .atMost(90, TimeUnit.SECONDS)
          .pollInterval(1, TimeUnit.SECONDS)
          .untilAsserted(
              () -> {
                ListObjectsV2Response response =
                    s3Client.listObjectsV2(ListObjectsV2Request.builder().bucket(BUCKET).build());
                Assert.assertTrue(
                    response.contents().stream()
                        .anyMatch(object -> object.key().endsWith(".tsfile")));
                Assert.assertTrue(
                    response.contents().stream()
                        .anyMatch(object -> object.key().endsWith(".resource")));
              });

      try (ResultSet resultSet =
          statement.executeQuery("SELECT device, value FROM sensor ORDER BY time")) {
        assertRow(resultSet, "d1", 11);
        assertRow(resultSet, "d1", 22);
        assertRow(resultSet, "d1", 33);
        Assert.assertFalse(resultSet.next());
      }
    }
  }

  /** First {@code .bin} still on a DataNode local object dir, or {@code null} if none. */
  private static File findLocalBin() throws Exception {
    for (DataNodeWrapper wrapper : EnvFactory.getEnv().getDataNodeWrapperList()) {
      File root = new File(wrapper.getDataNodeObjectDir());
      if (!root.exists()) {
        continue;
      }
      try (Stream<Path> stream = Files.walk(root.toPath())) {
        File found =
            stream
                .filter(Files::isRegularFile)
                .filter(path -> path.getFileName().toString().endsWith(".bin"))
                .map(Path::toFile)
                .findFirst()
                .orElse(null);
        if (found != null) {
          return found;
        }
      }
    }
    return null;
  }

  /** All MinIO object keys in the test bucket (TsFile, {@code .bin}, and anything else). */
  private static List<String> listAllKeys() {
    ListObjectsV2Response response =
        s3Client.listObjectsV2(ListObjectsV2Request.builder().bucket(BUCKET).build());
    return response.contents().stream().map(object -> object.key()).collect(Collectors.toList());
  }

  /** Leading path segment of a MinIO key, treated as the DataNode id ({@code {dnId}/...}). */
  private static String replicaPrefix(String key) {
    int slash = key.indexOf('/');
    return slash < 0 ? key : key.substring(0, slash);
  }

  /**
   * Whether {@code key} is an OBJECT {@code .bin} whose filename time falls in {@code partition}
   * ({@code time / 1000}, matching this IT's {@code timePartitionInterval}).
   */
  private static boolean isObjectBinInTimePartition(String key, long partition) {
    if (!key.endsWith(".bin")) {
      return false;
    }
    String name = key.substring(key.lastIndexOf('/') + 1);
    long time = parseObjectTimeFromName(name);
    return time >= 0 && time / 1000 == partition;
  }

  /** Timestamp prefix of {@code {time}_{...}.bin}; {@code -1} if the name is not that shape. */
  private static long parseObjectTimeFromName(String name) {
    if (!name.endsWith(".bin")) {
      return -1L;
    }
    String stem = name.substring(0, name.length() - ".bin".length());
    int underscore = stem.indexOf('_');
    String timePart = underscore < 0 ? stem : stem.substring(0, underscore);
    try {
      return Long.parseLong(timePart);
    } catch (NumberFormatException e) {
      return -1L;
    }
  }

  /** Keys that appeared after {@code before} and also match {@code keyFilter}. */
  private static Predicate<String> createdSince(Set<String> before, Predicate<String> keyFilter) {
    return key -> !before.contains(key) && keyFilter.test(key);
  }

  /**
   * Distinct DataNode prefixes among keys matching {@code keyFilter}. Coarse check only
   * (multi-replica vs already collapsed). Replica count per object must use {@link
   * #relativePathPrefixes}: IoTConsensus replicas flush independently, so {@code .bin} names may
   * differ, and devices may land in DataRegions whose leaders use different prefixes.
   */
  private static int countReplicaPrefixes(Predicate<String> keyFilter) {
    Set<String> prefixes = new HashSet<>();
    for (String key : listAllKeys()) {
      if (keyFilter.test(key)) {
        prefixes.add(replicaPrefix(key));
      }
    }
    return prefixes.size();
  }

  /** Strip the DataNode id prefix, leaving the relative object path shared across replicas. */
  private static String stripReplicaPrefix(String key) {
    int slash = key.indexOf('/');
    return slash < 0 ? key : key.substring(slash + 1);
  }

  /**
   * Group matching keys by relative object path and collect DataNode prefixes per path. This is the
   * per-object replica count: strip {@code {dataNodeId}/}, do not count prefixes globally.
   */
  private static Map<String, Set<String>> relativePathPrefixes(Predicate<String> keyFilter) {
    Map<String, Set<String>> prefixesByRelativePath = new HashMap<>();
    for (String key : listAllKeys()) {
      if (keyFilter.test(key)) {
        prefixesByRelativePath
            .computeIfAbsent(stripReplicaPrefix(key), ignored -> new HashSet<>())
            .add(replicaPrefix(key));
      }
    }
    return prefixesByRelativePath;
  }

  /**
   * Assert every matching relative object path has exactly {@code expectedReplicas} DataNode
   * prefixes, and that at least one object matched.
   */
  private static void assertExactReplicasPerObject(
      Predicate<String> keyFilter, int expectedReplicas, String message) {
    Map<String, Set<String>> prefixesByRelativePath = relativePathPrefixes(keyFilter);
    Assert.assertFalse(message + " (no matching objects)", prefixesByRelativePath.isEmpty());
    for (Map.Entry<String, Set<String>> entry : prefixesByRelativePath.entrySet()) {
      Assert.assertEquals(
          message + " object=" + entry.getKey() + " prefixes=" + entry.getValue(),
          expectedReplicas,
          entry.getValue().size());
    }
  }

  /**
   * Wait until this test's OBJECT {@code .bin} files in {@code partition} reach the expected
   * replica shape. {@code exact=false}: at least {@code expected} distinct DataNode prefixes
   * (pre-share multi-replica). {@code exact=true}: prefixes have collapsed below RF=3, and each
   * relative path has exactly {@code expected} last-tier replica(s).
   */
  private static void awaitPartitionObjectReplicas(
      Statement statement, Set<String> keysBefore, long partition, int expected, boolean exact)
      throws Exception {
    awaitPartitionObjectReplicas(statement, keysBefore, partition, expected, exact, 180);
  }

  /** Same as the overload above, with a custom timeout. */
  private static void awaitPartitionObjectReplicas(
      Statement statement,
      Set<String> keysBefore,
      long partition,
      int expected,
      boolean exact,
      int timeoutSeconds)
      throws Exception {
    Awaitility.await()
        .atMost(timeoutSeconds, TimeUnit.SECONDS)
        .pollInterval(2, TimeUnit.SECONDS)
        .untilAsserted(
            () -> {
              statement.execute("FLUSH");
              Predicate<String> filter =
                  createdSince(keysBefore, key -> isObjectBinInTimePartition(key, partition));
              if (exact) {
                int prefixes = countReplicaPrefixes(filter);
                Assert.assertTrue(
                    "partition-"
                        + partition
                        + " OBJECT .bin should collapse below RF=3 after share, prefixes="
                        + prefixes,
                    prefixes >= 1 && prefixes < 3);
                assertExactReplicasPerObject(
                    filter,
                    expected,
                    "partition-"
                        + partition
                        + " OBJECT .bin should share down to "
                        + expected
                        + " last-tier replica(s) per object");
              } else {
                int prefixes = countReplicaPrefixes(filter);
                Assert.assertTrue(
                    "partition-"
                        + partition
                        + " OBJECT .bin should exist on multiple MinIO prefixes (prefixes="
                        + prefixes
                        + ")",
                    prefixes >= expected);
              }
            });
  }

  /** JDBC {@code READ_OBJECT} as {@code 0x...}; falls back to hex-encoding {@code getBytes}. */
  private static String readObjectHex(ResultSet resultSet, int column) throws SQLException {
    String asString = resultSet.getString(column);
    if (asString != null) {
      return asString;
    }
    byte[] bytes = resultSet.getBytes(column);
    if (bytes == null) {
      return null;
    }
    StringBuilder hex = new StringBuilder("0x");
    for (byte b : bytes) {
      hex.append(String.format("%02x", b & 0xff));
    }
    return hex.toString();
  }

  /**
   * Pin {@code READ_OBJECT} to every live DataNode so a leader-only success cannot hide a follower
   * miss. After share, the leader reads its own last-tier {@code .bin}; followers remap the leader
   * OS object via {@code RemoteStorageBlock}.
   */
  private static void assertPayloadsOnEveryReplica(
      String database, String device, long time, String expectedHex) throws Exception {
    assertReadObjectOnEveryReplica(
        database,
        "SELECT READ_OBJECT(frame) FROM camera WHERE time = "
            + time
            + " AND device = '"
            + device
            + "'",
        expectedHex,
        "READ_OBJECT device=" + device + " time=" + time);
  }

  /** Same pin-every-DataNode check for {@code READ_OBJECT(frame, offset, length)}. */
  private static void assertOffsetReadOnEveryReplica(
      String database, String device, long time, long offset, int length, String expectedHex)
      throws Exception {
    assertReadObjectOnEveryReplica(
        database,
        "SELECT READ_OBJECT(frame, "
            + offset
            + ", "
            + length
            + ") FROM camera WHERE time = "
            + time
            + " AND device = '"
            + device
            + "'",
        expectedHex,
        "READ_OBJECT(offset=" + offset + ",length=" + length + ") device=" + device);
  }

  /**
   * Run {@code sql} against every live DataNode and require exactly one row whose payload equals
   * {@code expectedHex}.
   */
  private static void assertReadObjectOnEveryReplica(
      String database, String sql, String expectedHex, String what) throws Exception {
    List<String> failures = new ArrayList<>();
    int alive = 0;
    int success = 0;
    for (DataNodeWrapper wrapper : EnvFactory.getEnv().getDataNodeWrapperList()) {
      if (!wrapper.isAlive()) {
        continue;
      }
      alive++;
      try (Connection connection =
              EnvFactory.getEnv()
                  .getConnection(
                      wrapper,
                      SessionConfig.DEFAULT_USER,
                      SessionConfig.DEFAULT_PASSWORD,
                      BaseEnv.TABLE_SQL_DIALECT);
          Statement statement = connection.createStatement()) {
        statement.execute("USE " + database);
        try (ResultSet resultSet = statement.executeQuery(sql)) {
          if (!resultSet.next()) {
            failures.add("replica " + wrapper.getId() + ": missing row");
            continue;
          }
          String actual = readObjectHex(resultSet, 1);
          if (actual == null || actual.equals("0x") || actual.isEmpty()) {
            failures.add("replica " + wrapper.getId() + ": empty payload");
            continue;
          }
          if (!expectedHex.equals(actual)) {
            failures.add(
                "replica " + wrapper.getId() + ": expected " + expectedHex + " but was " + actual);
            continue;
          }
          if (resultSet.next()) {
            failures.add("replica " + wrapper.getId() + ": extra rows");
            continue;
          }
          success++;
        }
      } catch (SQLException e) {
        failures.add("replica " + wrapper.getId() + ": " + e.getMessage());
      }
    }
    Assert.assertTrue("no live DataNode to " + what, alive > 0);
    Assert.assertEquals(
        "every live DataNode must serve "
            + what
            + " expected="
            + expectedHex
            + " failures="
            + failures,
        alive,
        success);
  }

  /**
   * Pin {@code SELECT time, device} to every live DataNode and match {@code expectedRows} in order.
   * Checks visibility of rows, not OBJECT bytes.
   */
  private static void assertRowsEverywhere(String database, String[][] expectedRows)
      throws Exception {
    for (DataNodeWrapper wrapper : EnvFactory.getEnv().getDataNodeWrapperList()) {
      if (!wrapper.isAlive()) {
        continue;
      }
      try (Connection connection =
              EnvFactory.getEnv()
                  .getConnection(
                      wrapper,
                      SessionConfig.DEFAULT_USER,
                      SessionConfig.DEFAULT_PASSWORD,
                      BaseEnv.TABLE_SQL_DIALECT);
          Statement statement = connection.createStatement()) {
        statement.execute("USE " + database);
        try (ResultSet resultSet =
            statement.executeQuery("SELECT time, device FROM camera ORDER BY time, device")) {
          for (String[] expected : expectedRows) {
            Assert.assertTrue(
                "replica " + wrapper.getId() + " missing row " + expected[0] + "/" + expected[1],
                resultSet.next());
            Assert.assertEquals(
                "replica " + wrapper.getId() + " time mismatch",
                Long.parseLong(expected[0]),
                resultSet.getLong(1));
            Assert.assertEquals(
                "replica " + wrapper.getId() + " device mismatch",
                expected[1],
                resultSet.getString(2));
          }
          Assert.assertFalse("replica " + wrapper.getId() + " extra rows", resultSet.next());
        }
      }
    }
  }

  /** Wait until at least one new OBJECT {@code .bin} is on MinIO and the local tier has none. */
  private static void awaitObjectMigratedToMinIO() {
    awaitObjectMigratedToMinIOAndKeys();
  }

  /**
   * Same as {@link #awaitObjectMigratedToMinIO()}, returning the newly appeared remote {@code .bin}
   * keys.
   */
  private static Set<String> awaitObjectMigratedToMinIOAndKeys() {
    Set<String> remoteBinsBefore = new HashSet<>(listRemoteBinKeys());
    Awaitility.await()
        .atMost(90, TimeUnit.SECONDS)
        .pollInterval(1, TimeUnit.SECONDS)
        .untilAsserted(
            () -> {
              Assert.assertTrue(
                  "object .bin should be migrated to MinIO",
                  listRemoteBinKeys().size() > remoteBinsBefore.size());
              Assert.assertNull(
                  "source object on local tier should be deleted after migration", findLocalBin());
            });
    Set<String> migrated = new HashSet<>(listRemoteBinKeys());
    migrated.removeAll(remoteBinsBefore);
    return migrated;
  }

  /** MinIO keys in this bucket that end with {@code .bin}. */
  private static List<String> listRemoteBinKeys() {
    ListObjectsV2Response response =
        s3Client.listObjectsV2(ListObjectsV2Request.builder().bucket(BUCKET).build());
    return response.contents().stream()
        .map(object -> object.key())
        .filter(key -> key.endsWith(".bin"))
        .collect(Collectors.toList());
  }

  /** Consume the next row and assert {@code device}, {@code value}. */
  private static void assertRow(ResultSet resultSet, String device, int value) throws Exception {
    Assert.assertTrue(resultSet.next());
    Assert.assertEquals(device, resultSet.getString(1));
    Assert.assertEquals(value, resultSet.getInt(2));
  }

  /** Path-style S3 client aimed at the IT MinIO endpoint. */
  private static S3Client createS3Client() {
    return S3Client.builder()
        .endpointOverride(URI.create(MINIO_ENDPOINT))
        .region(Region.of(REGION))
        .credentialsProvider(
            StaticCredentialsProvider.create(AwsBasicCredentials.create(ACCESS_KEY, SECRET_KEY)))
        .forcePathStyle(true)
        .build();
  }

  /** Delete every object in the test bucket, then the bucket itself. */
  private static void deleteBucket() {
    ListObjectsV2Response response =
        s3Client.listObjectsV2(ListObjectsV2Request.builder().bucket(BUCKET).build());
    response
        .contents()
        .forEach(
            object ->
                s3Client.deleteObject(
                    DeleteObjectRequest.builder().bucket(BUCKET).key(object.key()).build()));
    s3Client.deleteBucket(DeleteBucketRequest.builder().bucket(BUCKET).build());
  }
}
