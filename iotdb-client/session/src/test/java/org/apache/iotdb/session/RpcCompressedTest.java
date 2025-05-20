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

package org.apache.iotdb.session;

import org.apache.iotdb.isession.ITableSession;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.BitMap;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class RpcCompressedTest {

  @Mock private ITableSession session;

  @Mock private SessionConnection sessionConnection;

  @Before
  public void setUp() throws IoTDBConnectionException, StatementExecutionException {
    MockitoAnnotations.initMocks(this);
    List<String> nodeUrls = Arrays.asList("127.0.0.1:6667");
    session =
        new TableSessionBuilder()
            .nodeUrls(nodeUrls)
            .username("root")
            .password("root")
            .enableCompression(false)
            .enableRedirection(true)
            .enableAutoFetch(false)
            .isCompressed(true)
            .withCompressionType(CompressionType.SNAPPY)
            .withBooleanEncoding(TSEncoding.PLAIN)
            .withInt32Encoding(TSEncoding.PLAIN)
            .withInt64Encoding(TSEncoding.PLAIN)
            .withFloatEncoding(TSEncoding.PLAIN)
            .withDoubleEncoding(TSEncoding.PLAIN)
            .withBlobEncoding(TSEncoding.PLAIN)
            .withStringEncoding(TSEncoding.PLAIN)
            .withTextEncoding(TSEncoding.PLAIN)
            .withDateEncoding(TSEncoding.PLAIN)
            .withTimeStampEncoding(TSEncoding.PLAIN)
            .build();
  }

  @After
  public void tearDown() throws IoTDBConnectionException {
    // Close the session pool after each test
    if (null != session) {
      session.close();
    }
  }

  @Test
  public void testRpcDecode() throws IoTDBConnectionException, StatementExecutionException {
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

    long[] timestamp = new long[] {1L, 2L};
    Object[] values = new Object[6];
    values[0] = new int[] {1, 2};
    values[1] = new long[] {1L, 2L};
    values[2] = new float[] {1.1f, 1.2f};
    values[3] = new double[] {0.707, 0.708};
    values[4] =
        new Binary[] {new Binary(new byte[] {(byte) 32}), new Binary(new byte[] {(byte) 16})};
    values[5] = new boolean[] {true, false};
    BitMap[] partBitMap = new BitMap[6];
    Tablet tablet = new Tablet("Table_0", schemas, timestamp, values, partBitMap, 2);

    session.executeNonQueryStatement("create database IF NOT EXISTS db_0");
    session.executeNonQueryStatement("use db_0");
    session.insert(tablet);
  }
}
