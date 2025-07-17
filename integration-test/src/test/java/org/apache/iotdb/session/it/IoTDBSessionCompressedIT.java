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
import java.util.List;
import java.util.stream.Collectors;

public class IoTDBSessionCompressedIT {

  private static ITableSession session1;
  private static ITableSession session2;
  private static ITableSession session3;
  private static ITableSession session4;
  private static ITableSession session5;

  @BeforeClass
  public static void setUpClass() throws IoTDBConnectionException {
    EnvFactory.getEnv().initClusterEnvironment();

    List<String> nodeUrls =
        EnvFactory.getEnv().getDataNodeWrapperList().stream()
            .map(DataNodeWrapper::getIpAndPortString)
            .collect(Collectors.toList());
    //    List<String> nodeUrls = Collections.singletonList("127.0.0.1:6667");
    session1 =
        new TableSessionBuilder()
            .nodeUrls(nodeUrls)
            .username(CommonDescriptor.getInstance().getConfig().getAdminName())
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
    session2 =
        new TableSessionBuilder()
            .nodeUrls(nodeUrls)
            .username(CommonDescriptor.getInstance().getConfig().getAdminName())
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
    session3 =
        new TableSessionBuilder()
            .nodeUrls(nodeUrls)
            .username(CommonDescriptor.getInstance().getConfig().getAdminName())
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
    session4 =
        new TableSessionBuilder()
            .nodeUrls(nodeUrls)
            .username(CommonDescriptor.getInstance().getConfig().getAdminName())
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
    session5 =
        new TableSessionBuilder()
            .nodeUrls(nodeUrls)
            .username(CommonDescriptor.getInstance().getConfig().getAdminName())
            .password(CommonDescriptor.getInstance().getConfig().getAdminPassword())
            .enableCompression(false)
            .enableRedirection(true)
            .enableAutoFetch(false)
            .enableCompaction(false)
            .build();
  }

  @AfterClass
  public static void tearDownClass() throws IoTDBConnectionException {
    if (session1 != null) {
      session1.close();
    }
    if (session2 != null) {
      session2.close();
    }
    if (session3 != null) {
      session3.close();
    }
    if (session4 != null) {
      session4.close();
    }
    if (session5 != null) {
      session5.close();
    }
    EnvFactory.getEnv().cleanClusterEnvironment();
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

    session1.executeNonQueryStatement("create database IF NOT EXISTS dbTest_0");
    session1.executeNonQueryStatement("use dbTest_0");
    session2.executeNonQueryStatement("use dbTest_0");
    session3.executeNonQueryStatement("use dbTest_0");
    session4.executeNonQueryStatement("use dbTest_0");
    session5.executeNonQueryStatement("use dbTest_0");

    // 1. insert
    session1.insert(tablet);
    session2.insert(tablet);
    session3.insert(tablet);
    session4.insert(tablet);
    session5.insert(tablet);

    // 2. assert
    SessionDataSet sessionDataSet1 =
        session1.executeQueryStatement("select * from dbTest_0." + tableName);
    SessionDataSet sessionDataSet2 =
        session2.executeQueryStatement("select * from dbTest_0." + tableName);
    SessionDataSet sessionDataSet3 =
        session3.executeQueryStatement("select * from dbTest_0." + tableName);
    SessionDataSet sessionDataSet4 =
        session4.executeQueryStatement("select * from dbTest_0." + tableName);
    SessionDataSet sessionDataSet5 =
        session5.executeQueryStatement("select * from dbTest_0." + tableName);

    if (sessionDataSet1.hasNext()) {
      RowRecord next = sessionDataSet1.next();
      Assert.assertEquals(3L, next.getFields().get(0).getLongV());
      Assert.assertEquals(1, next.getFields().get(1).getIntV());
      Assert.assertEquals(1L, next.getFields().get(2).getLongV());
      Assert.assertEquals(1.1f, next.getFields().get(3).getFloatV(), 0.01);
      Assert.assertEquals(0.707, next.getFields().get(4).getDoubleV(), 0.01);
      Assert.assertEquals(new Binary(new byte[] {(byte) 32}), next.getFields().get(5).getBinaryV());
      Assert.assertEquals(true, next.getFields().get(6).getBoolV());
      Assert.assertEquals(new Binary(new byte[] {(byte) 32}), next.getFields().get(7).getBinaryV());
      Assert.assertEquals(new Binary(new byte[] {(byte) 32}), next.getFields().get(8).getBinaryV());
    }
    if (sessionDataSet2.hasNext()) {
      RowRecord next = sessionDataSet2.next();
      Assert.assertEquals(3L, next.getFields().get(0).getLongV());
      Assert.assertEquals(1, next.getFields().get(1).getIntV());
      Assert.assertEquals(1L, next.getFields().get(2).getLongV());
      Assert.assertEquals(1.1f, next.getFields().get(3).getFloatV(), 0.01);
      Assert.assertEquals(0.707, next.getFields().get(4).getDoubleV(), 0.01);
      Assert.assertEquals(new Binary(new byte[] {(byte) 32}), next.getFields().get(5).getBinaryV());
      Assert.assertEquals(true, next.getFields().get(6).getBoolV());
      Assert.assertEquals(new Binary(new byte[] {(byte) 32}), next.getFields().get(7).getBinaryV());
      Assert.assertEquals(new Binary(new byte[] {(byte) 32}), next.getFields().get(8).getBinaryV());
    }
    if (sessionDataSet3.hasNext()) {
      RowRecord next = sessionDataSet3.next();
      Assert.assertEquals(3L, next.getFields().get(0).getLongV());
      Assert.assertEquals(1, next.getFields().get(1).getIntV());
      Assert.assertEquals(1L, next.getFields().get(2).getLongV());
      Assert.assertEquals(1.1f, next.getFields().get(3).getFloatV(), 0.01);
      Assert.assertEquals(0.707, next.getFields().get(4).getDoubleV(), 0.01);
      Assert.assertEquals(new Binary(new byte[] {(byte) 32}), next.getFields().get(5).getBinaryV());
      Assert.assertEquals(true, next.getFields().get(6).getBoolV());
      Assert.assertEquals(new Binary(new byte[] {(byte) 32}), next.getFields().get(7).getBinaryV());
      Assert.assertEquals(new Binary(new byte[] {(byte) 32}), next.getFields().get(8).getBinaryV());
    }
    if (sessionDataSet4.hasNext()) {
      RowRecord next = sessionDataSet4.next();
      Assert.assertEquals(3L, next.getFields().get(0).getLongV());
      Assert.assertEquals(1, next.getFields().get(1).getIntV());
      Assert.assertEquals(1L, next.getFields().get(2).getLongV());
      Assert.assertEquals(1.1f, next.getFields().get(3).getFloatV(), 0.01);
      Assert.assertEquals(0.707, next.getFields().get(4).getDoubleV(), 0.01);
      Assert.assertEquals(new Binary(new byte[] {(byte) 32}), next.getFields().get(5).getBinaryV());
      Assert.assertEquals(true, next.getFields().get(6).getBoolV());
      Assert.assertEquals(new Binary(new byte[] {(byte) 32}), next.getFields().get(7).getBinaryV());
      Assert.assertEquals(new Binary(new byte[] {(byte) 32}), next.getFields().get(8).getBinaryV());
    }
    if (sessionDataSet5.hasNext()) {
      RowRecord next = sessionDataSet5.next();
      Assert.assertEquals(3L, next.getFields().get(0).getLongV());
      Assert.assertEquals(1, next.getFields().get(1).getIntV());
      Assert.assertEquals(1L, next.getFields().get(2).getLongV());
      Assert.assertEquals(1.1f, next.getFields().get(3).getFloatV(), 0.01);
      Assert.assertEquals(0.707, next.getFields().get(4).getDoubleV(), 0.01);
      Assert.assertEquals(new Binary(new byte[] {(byte) 32}), next.getFields().get(5).getBinaryV());
      Assert.assertEquals(true, next.getFields().get(6).getBoolV());
      Assert.assertEquals(new Binary(new byte[] {(byte) 32}), next.getFields().get(7).getBinaryV());
      Assert.assertEquals(new Binary(new byte[] {(byte) 32}), next.getFields().get(8).getBinaryV());
    }
  }
}
