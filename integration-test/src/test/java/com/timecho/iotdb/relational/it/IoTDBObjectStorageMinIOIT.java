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
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.DeleteBucketRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;

import java.net.URI;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

@RunWith(IoTDBTestRunner.class)
@Category({TableLocalStandaloneIT.class})
public class IoTDBObjectStorageMinIOIT {

  private static final String MINIO_ENDPOINT = "http://11.101.17.170:9000";
  private static final String ACCESS_KEY = "minioadmin";
  private static final String SECRET_KEY = "fce2e70eb69fab571024cf9a5526ebd700594e99fb3cc902";
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
        .endpointOverride(URI.create(MINIO_ENDPOINT))
        .region(Region.of(REGION))
        .credentialsProvider(
            StaticCredentialsProvider.create(AwsBasicCredentials.create(ACCESS_KEY, SECRET_KEY)))
        .forcePathStyle(true)
        .build();
  }

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
