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
package org.apache.iotdb.session.it;

import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.isession.ITableSession;
import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.env.cluster.node.DataNodeWrapper;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.TableSessionBuilder;

import org.apache.tsfile.enums.ColumnCategory;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.read.common.RowRecord;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.BitMap;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class IoTDBSessionCompressedIT {

  private static List<ITableSession> sessions;

  @BeforeClass
  public static void setUpClass() throws IoTDBConnectionException {
    EnvFactory.getEnv().initClusterEnvironment();

    List<String> nodeUrls =
        EnvFactory.getEnv().getDataNodeWrapperList().stream()
            .map(DataNodeWrapper::getIpAndPortString)
            .collect(Collectors.toList());
    ITableSession session1 =
        new TableSessionBuilder()
            .nodeUrls(nodeUrls)
            .username(CommonDescriptor.getInstance().getConfig().getDefaultAdminName())
            .password(CommonDescriptor.getInstance().getConfig().getAdminPassword())
            .enableCompression(true)
            .enableRedirection(true)
            .enableAutoFetch(false)
            .withCompressionType(CompressionType.SNAPPY)
            .withBooleanEncoding(TSEncoding.PLAIN)
            .withInt32Encoding(TSEncoding.CHIMP)
            .withInt64Encoding(TSEncoding.CHIMP)
            .withFloatEncoding(TSEncoding.CHIMP)
            .withDoubleEncoding(TSEncoding.CHIMP)
            .withBlobEncoding(TSEncoding.PLAIN)
            .withStringEncoding(TSEncoding.PLAIN)
            .withTextEncoding(TSEncoding.PLAIN)
            .withDateEncoding(TSEncoding.PLAIN)
            .withTimeStampEncoding(TSEncoding.PLAIN)
            .build();
    ITableSession session2 =
        new TableSessionBuilder()
            .nodeUrls(nodeUrls)
            .username(CommonDescriptor.getInstance().getConfig().getDefaultAdminName())
            .password(CommonDescriptor.getInstance().getConfig().getAdminPassword())
            .enableCompression(true)
            .enableRedirection(true)
            .enableAutoFetch(false)
            .withCompressionType(CompressionType.SNAPPY)
            .withBooleanEncoding(TSEncoding.PLAIN)
            .withInt32Encoding(TSEncoding.SPRINTZ)
            .withInt64Encoding(TSEncoding.SPRINTZ)
            .withFloatEncoding(TSEncoding.RLBE)
            .withDoubleEncoding(TSEncoding.RLBE)
            .withBlobEncoding(TSEncoding.PLAIN)
            .withStringEncoding(TSEncoding.PLAIN)
            .withTextEncoding(TSEncoding.PLAIN)
            .withDateEncoding(TSEncoding.PLAIN)
            .withTimeStampEncoding(TSEncoding.SPRINTZ)
            .build();
    ITableSession session3 =
        new TableSessionBuilder()
            .nodeUrls(nodeUrls)
            .username(CommonDescriptor.getInstance().getConfig().getDefaultAdminName())
            .password(CommonDescriptor.getInstance().getConfig().getAdminPassword())
            .enableCompression(true)
            .enableRedirection(true)
            .enableAutoFetch(false)
            .withCompressionType(CompressionType.GZIP)
            .withBooleanEncoding(TSEncoding.RLE)
            .withInt32Encoding(TSEncoding.TS_2DIFF)
            .withInt64Encoding(TSEncoding.RLE)
            .withFloatEncoding(TSEncoding.TS_2DIFF)
            .withDoubleEncoding(TSEncoding.RLE)
            .withBlobEncoding(TSEncoding.PLAIN)
            .withStringEncoding(TSEncoding.PLAIN)
            .withTextEncoding(TSEncoding.PLAIN)
            .withDateEncoding(TSEncoding.RLE)
            .withTimeStampEncoding(TSEncoding.RLE)
            .build();
    ITableSession session4 =
        new TableSessionBuilder()
            .nodeUrls(nodeUrls)
            .username(CommonDescriptor.getInstance().getConfig().getDefaultAdminName())
            .password(CommonDescriptor.getInstance().getConfig().getAdminPassword())
            .enableCompression(true)
            .enableRedirection(true)
            .enableAutoFetch(false)
            .withCompressionType(CompressionType.LZMA2)
            .withBooleanEncoding(TSEncoding.PLAIN)
            .withInt32Encoding(TSEncoding.GORILLA)
            .withInt64Encoding(TSEncoding.ZIGZAG)
            .withFloatEncoding(TSEncoding.GORILLA)
            .withDoubleEncoding(TSEncoding.GORILLA)
            .withBlobEncoding(TSEncoding.PLAIN)
            .withStringEncoding(TSEncoding.PLAIN)
            .withTextEncoding(TSEncoding.PLAIN)
            .withDateEncoding(TSEncoding.RLE)
            .withTimeStampEncoding(TSEncoding.ZIGZAG)
            .build();
    ITableSession session5 =
        new TableSessionBuilder()
            .nodeUrls(nodeUrls)
            .username(CommonDescriptor.getInstance().getConfig().getDefaultAdminName())
            .password(CommonDescriptor.getInstance().getConfig().getAdminPassword())
            .enableCompression(false)
            .enableRedirection(true)
            .enableAutoFetch(false)
            .enableCompaction(false)
            .build();
    sessions = Arrays.asList(session1, session2, session3, session4, session5);
  }

  @AfterClass
  public static void tearDownClass() throws IoTDBConnectionException {
    for (ITableSession session : sessions) {
      session.close();
    }
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void testAllNullColumn() throws IoTDBConnectionException, StatementExecutionException {
    Tablet tablet =
        new Tablet(
            "t1",
            Arrays.asList("tag1", "attr1", "s1"),
            Arrays.asList(TSDataType.STRING, TSDataType.STRING, TSDataType.DATE),
            Arrays.asList(ColumnCategory.TAG, ColumnCategory.ATTRIBUTE, ColumnCategory.FIELD),
            100);
    for (int i = 0; i < 10; i++) {
      tablet.addTimestamp(i, i);
      tablet.addValue("tag1", i, "d1");
      tablet.addValue("attr1", i, "blue");
    }
    for (ITableSession session : sessions) {
      session.executeNonQueryStatement("CREATE DATABASE IF NOT EXISTS test");
      session.executeNonQueryStatement("USE test");
      session.insert(tablet);
    }
  }

  @Test
  public void testAllNull() throws IoTDBConnectionException, StatementExecutionException {
    Tablet tablet =
        new Tablet(
            "t1",
            Arrays.asList("tag1", "attr1", "s1"),
            Arrays.asList(TSDataType.STRING, TSDataType.STRING, TSDataType.DATE),
            Arrays.asList(ColumnCategory.TAG, ColumnCategory.ATTRIBUTE, ColumnCategory.FIELD),
            100);
    for (int i = 0; i < 10; i++) {
      tablet.addTimestamp(i, i);
    }
    for (ITableSession session : sessions) {
      session.executeNonQueryStatement("CREATE DATABASE IF NOT EXISTS test");
      session.executeNonQueryStatement("USE test");
      session.insert(tablet);
    }
  }

  @Test
  public void testRpcCompressed() throws IoTDBConnectionException, StatementExecutionException {
    List<IMeasurementSchema> schemas = new ArrayList<>();
    MeasurementSchema schema = new MeasurementSchema();
    schema.setMeasurementName("pressure0");
    schema.setDataType(TSDataType.INT32);
    schema.setCompressionType(CompressionType.SNAPPY);
    schema.setEncoding(TSEncoding.PLAIN);
    schemas.add(schema);
    schema = new MeasurementSchema();
    schema.setMeasurementName("pressure1");
    schema.setDataType(TSDataType.INT64);
    schema.setCompressionType(CompressionType.SNAPPY);
    schema.setEncoding(TSEncoding.PLAIN);
    schemas.add(schema);
    schema = new MeasurementSchema();
    schema.setMeasurementName("pressure2");
    schema.setDataType(TSDataType.FLOAT);
    schema.setCompressionType(CompressionType.SNAPPY);
    schema.setEncoding(TSEncoding.PLAIN);
    schemas.add(schema);
    schema = new MeasurementSchema();
    schema.setMeasurementName("pressure3");
    schema.setDataType(TSDataType.DOUBLE);
    schema.setCompressionType(CompressionType.SNAPPY);
    schema.setEncoding(TSEncoding.PLAIN);
    schemas.add(schema);
    schema = new MeasurementSchema();
    schema.setMeasurementName("pressure4");
    schema.setDataType(TSDataType.TEXT);
    schema.setCompressionType(CompressionType.SNAPPY);
    schema.setEncoding(TSEncoding.PLAIN);
    schemas.add(schema);
    schema = new MeasurementSchema();
    schema.setMeasurementName("pressure5");
    schema.setDataType(TSDataType.BOOLEAN);
    schema.setCompressionType(CompressionType.SNAPPY);
    schema.setEncoding(TSEncoding.PLAIN);
    schemas.add(schema);
    schema = new MeasurementSchema();
    schema.setMeasurementName("pressure6");
    schema.setDataType(TSDataType.STRING);
    schema.setCompressionType(CompressionType.SNAPPY);
    schema.setEncoding(TSEncoding.PLAIN);
    schemas.add(schema);
    schema = new MeasurementSchema();
    schema.setMeasurementName("pressure7");
    schema.setDataType(TSDataType.BLOB);
    schema.setCompressionType(CompressionType.SNAPPY);
    schema.setEncoding(TSEncoding.PLAIN);
    schemas.add(schema);
    schema = new MeasurementSchema();
    schema.setMeasurementName("pressure8");
    schema.setDataType(TSDataType.DATE);
    schema.setCompressionType(CompressionType.SNAPPY);
    schema.setEncoding(TSEncoding.PLAIN);
    schemas.add(schema);

    long[] timestamp = new long[] {3L, 4L, 5L, 6L};
    Object[] values = new Object[9];
    values[0] = new int[] {1, 2, 8, 15};
    values[1] = new long[] {1L, 2L, 8L, 15L};
    values[2] = new float[] {1.1f, 1.2f, 8.8f, 15.5f};
    values[3] = new double[] {0.707, 0.708, 8.8, 15.5};
    values[4] =
        new Binary[] {
          new Binary(new byte[] {(byte) 32}),
          new Binary(new byte[] {(byte) 16}),
          new Binary(new byte[] {(byte) 1}),
          new Binary(new byte[] {(byte) 56})
        };
    values[5] = new boolean[] {true, false, true, false};
    values[6] =
        new Binary[] {
          new Binary(new byte[] {(byte) 32}),
          new Binary(new byte[] {(byte) 16}),
          new Binary(new byte[] {(byte) 1}),
          new Binary(new byte[] {(byte) 56})
        };
    values[7] =
        new Binary[] {
          new Binary(new byte[] {(byte) 32}),
          new Binary(new byte[] {(byte) 16}),
          new Binary(new byte[] {(byte) 1}),
          new Binary(new byte[] {(byte) 56})
        };
    values[8] =
        new LocalDate[] {
          LocalDate.of(1999, 1, 1),
          LocalDate.of(1999, 1, 2),
          LocalDate.of(1999, 1, 3),
          LocalDate.of(1999, 1, 4),
        };
    BitMap[] partBitMap = new BitMap[9];

    String tableName = "table_13";
    Tablet tablet = new Tablet(tableName, schemas, timestamp, values, partBitMap, 4);

    sessions.get(0).executeNonQueryStatement("create database IF NOT EXISTS dbTest_0");
    for (ITableSession session : sessions) {
      session.executeNonQueryStatement("use dbTest_0");
    }

    // 1. insert
    for (ITableSession session : sessions) {
      session.insert(tablet);
    }

    // 2. assert
    for (ITableSession session : sessions) {
      try (SessionDataSet sessionDataSet =
          session.executeQueryStatement("select * from dbTest_0." + tableName)) {
        if (sessionDataSet.hasNext()) {
          RowRecord next = sessionDataSet.next();
          Assert.assertEquals(3L, next.getFields().get(0).getLongV());
          Assert.assertEquals(1, next.getFields().get(1).getIntV());
          Assert.assertEquals(1L, next.getFields().get(2).getLongV());
          Assert.assertEquals(1.1f, next.getFields().get(3).getFloatV(), 0.01);
          Assert.assertEquals(0.707, next.getFields().get(4).getDoubleV(), 0.01);
          Assert.assertEquals(
              new Binary(new byte[] {(byte) 32}), next.getFields().get(5).getBinaryV());
          Assert.assertTrue(next.getFields().get(6).getBoolV());
          Assert.assertEquals(
              new Binary(new byte[] {(byte) 32}), next.getFields().get(7).getBinaryV());
          Assert.assertEquals(
              new Binary(new byte[] {(byte) 32}), next.getFields().get(8).getBinaryV());
        }
      }
    }
  }
}
