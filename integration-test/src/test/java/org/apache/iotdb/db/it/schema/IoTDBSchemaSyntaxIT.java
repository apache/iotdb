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

package org.apache.iotdb.db.it.schema;

import org.apache.iotdb.isession.ISession;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;
import org.apache.iotdb.util.AbstractSchemaIT;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.write.record.Tablet;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runners.Parameterized;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

@Category({LocalStandaloneIT.class, ClusterIT.class})
public class IoTDBSchemaSyntaxIT extends AbstractSchemaIT {
  public IoTDBSchemaSyntaxIT(SchemaTestMode schemaTestMode) {
    super(schemaTestMode);
  }

  @Parameterized.BeforeParam
  public static void before() throws Exception {
    setUpEnvironment();
    EnvFactory.getEnv().initClusterEnvironment();
  }

  @Parameterized.AfterParam
  public static void after() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
    tearDownEnvironment();
  }

  @After
  public void tearDown() throws Exception {
    clearSchema();
  }

  @Test
  public void testInvalidCreation() throws Exception {
    final List<String> invalidSQLs =
        Arrays.asList(
            "CREATE TIMESERIES root.sg1.d1.s1 OBJECT",
            "CREATE ALIGNED TIMESERIES root.sg1.d1.vector1(s1 OBJECT encoding=PLAIN compressor=UNCOMPRESSED,s2 INT64 encoding=RLE)",
            "create schema template t1 (s2 OBJECT encoding=RLE, s3 INT64 encoding=RLE compression=SNAPPY)");

    try (final Connection connection = EnvFactory.getEnv().getConnection();
        final Statement statement = connection.createStatement()) {
      for (final String invalidSQL : invalidSQLs) {
        try {
          statement.execute(invalidSQL);
          Assert.fail();
        } catch (final Exception ignore) {
          // Expected
        }
      }
    }

    final String testObject =
        System.getProperty("user.dir")
            + File.separator
            + "target"
            + File.separator
            + "test-classes"
            + File.separator
            + "object-example.pt";
    final byte[] objectBytes = Files.readAllBytes(Paths.get(testObject));
    final List<byte[]> objectSegments = new ArrayList<>();
    for (int i = 0; i < objectBytes.length; i += 512) {
      objectSegments.add(Arrays.copyOfRange(objectBytes, i, Math.min(i + 512, objectBytes.length)));
    }
    try (final ISession session = EnvFactory.getEnv().getSessionConnection()) {
      // insert table data by tablet
      final List<String> columnNameList =
          Arrays.asList("region_id", "plant_id", "device_id", "temperature", "file");
      final List<TSDataType> dataTypeList =
          Arrays.asList(
              TSDataType.STRING,
              TSDataType.STRING,
              TSDataType.STRING,
              TSDataType.FLOAT,
              TSDataType.OBJECT);
      final Tablet tablet = new Tablet(columnNameList, dataTypeList);
      tablet.setDeviceId("root.test.objectDevice");
      for (int i = 0; i < columnNameList.size() - 1; i++) {
        int rowIndex = tablet.getRowSize();
        tablet.addTimestamp(rowIndex, 1);
        tablet.addValue(rowIndex, 0, "1");
        tablet.addValue(rowIndex, 1, "5");
        tablet.addValue(rowIndex, 2, "3");
        tablet.addValue(rowIndex, 3, 37.6F);
        tablet.addValue(rowIndex, 4, false, i * 512L, objectSegments.get(i));
      }
      try {
        session.insertTablet(tablet);
        Assert.fail();
      } catch (final Exception ignore) {
        // Expected
      }
      try {
        session.insertAlignedTablet(tablet);
        Assert.fail();
      } catch (final Exception ignore) {
        // Expected
      }
      try {
        session.createMultiTimeseries(
            Collections.singletonList("root.sg1.d1.s1"),
            Collections.singletonList(TSDataType.OBJECT),
            Collections.singletonList(TSEncoding.PLAIN),
            Collections.singletonList(CompressionType.LZ4),
            null,
            null,
            null,
            null);
      } catch (final Exception ignore) {
        // Expected
      }
      tablet.reset();
    }
  }
}
