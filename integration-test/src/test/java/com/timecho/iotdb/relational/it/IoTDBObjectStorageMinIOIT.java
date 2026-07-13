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

import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.TableLocalStandaloneIT;
import org.apache.iotdb.itbase.env.DataNodeConfig;

import org.awaitility.Awaitility;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;

import java.net.URI;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.concurrent.TimeUnit;

@RunWith(IoTDBTestRunner.class)
@Category({TableLocalStandaloneIT.class})
public class IoTDBObjectStorageMinIOIT {

  private static final String ACCESS_KEY = "minioadmin";
  private static final String SECRET_KEY = "minioadmin";
  private static final String BUCKET = "iotdb-data";
  private static final String REGION = "us-east-1";
  private static final String DOCKER_API_VERSION_PROPERTY = "api.version";
  private static final String DEFAULT_DOCKER_API_VERSION = "1.44";
  private static final int MINIO_PORT = 9000;

  private static final GenericContainer<?> MINIO =
      new GenericContainer<>(DockerImageName.parse("minio/minio:RELEASE.2025-04-22T22-12-26Z"))
          .withEnv("MINIO_ROOT_USER", ACCESS_KEY)
          .withEnv("MINIO_ROOT_PASSWORD", SECRET_KEY)
          .withCommand("server", "/data")
          .withExposedPorts(MINIO_PORT)
          .waitingFor(Wait.forHttp("/minio/health/ready").forPort(MINIO_PORT));

  private static S3Client s3Client;
  private static boolean environmentStarted;

  @BeforeClass
  public static void setUp() throws Exception {
    if (System.getProperty(DOCKER_API_VERSION_PROPERTY) == null) {
      System.setProperty(DOCKER_API_VERSION_PROPERTY, DEFAULT_DOCKER_API_VERSION);
    }
    Assume.assumeTrue(DockerClientFactory.instance().isDockerAvailable());
    MINIO.start();
    s3Client = createS3Client();
    s3Client.createBucket(CreateBucketRequest.builder().bucket(BUCKET).build());

    DataNodeConfig dataNodeConfig = EnvFactory.getEnv().getConfig().getDataNodeConfig();
    dataNodeConfig
        .setDnDataDirs("data/datanode/data;OBJECT_STORAGE")
        .setObjectStorageType("AWS_S3")
        .setObjectStorageEndpoint(getEndpoint())
        .setObjectStorageRegion(REGION)
        .setObjectStorageBucket(BUCKET)
        .setObjectStorageAccessKey(ACCESS_KEY)
        .setObjectStorageAccessSecret(SECRET_KEY)
        .setEnablePathStyleAccess(true);
    EnvFactory.getEnv().getConfig().getDataNodeCommonConfig().setTierTTLInMs("0;-1");
    EnvFactory.getEnv().initClusterEnvironment(1, 1);
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
        s3Client.close();
      }
      MINIO.stop();
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

  private static void assertRow(ResultSet resultSet, String device, int value) throws Exception {
    Assert.assertTrue(resultSet.next());
    Assert.assertEquals(device, resultSet.getString(1));
    Assert.assertEquals(value, resultSet.getInt(2));
  }

  private static S3Client createS3Client() {
    return S3Client.builder()
        .endpointOverride(URI.create(getEndpoint()))
        .region(Region.of(REGION))
        .credentialsProvider(
            StaticCredentialsProvider.create(AwsBasicCredentials.create(ACCESS_KEY, SECRET_KEY)))
        .forcePathStyle(true)
        .build();
  }

  private static String getEndpoint() {
    return "http://" + MINIO.getHost() + ':' + MINIO.getMappedPort(MINIO_PORT);
  }
}
