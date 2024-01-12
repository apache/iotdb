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

package org.apache.iotdb.pipe.it;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.client.sync.SyncConfigNodeIServiceClient;
import org.apache.iotdb.confignode.rpc.thrift.TCreatePipeReq;
import org.apache.iotdb.db.it.utils.TestUtils;
import org.apache.iotdb.isession.ISession;
import org.apache.iotdb.it.env.cluster.node.DataNodeWrapper;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.MultiClusterIT2;
import org.apache.iotdb.pipe.PipeEnvironmentException;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.BitMap;
import org.apache.iotdb.tsfile.write.record.Tablet;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({MultiClusterIT2.class})
public class IoTDBPipeNullValueIT extends AbstractPipeDualIT {

  // Test dimensions:
  // 1. is or not aligned
  // 2. is or not parsed
  // 3. session insertRecord, session insertTablet, SQL insert
  // 4. partial null, all null
  // 5. one row or more (TODO)
  // 6. more data types (TODO)

  private enum InsertType {
    SESSION_INSERT_RECORD,
    SESSION_INSERT_TABLET,
    SQL_INSERT,
  }

  private static final Map<InsertType, Consumer<Boolean>> INSERT_NULL_VALUE_MAP = new HashMap<>();

  private static final List<String> CREATE_TIMESERIES_SQL =
      Arrays.asList(
          "create timeseries root.sg.d1.s0 with datatype=float",
          "create timeseries root.sg.d1.s1 with datatype=float");

  private static final List<String> CREATE_ALIGNED_TIMESERIES_SQL =
      Collections.singletonList("create aligned timeseries root.sg.d1(s0 float, s1 float)");

  private final String deviceId = "root.sg.d1";
  private final List<String> measurements = Arrays.asList("s0", "s1");
  private final List<TSDataType> types = Arrays.asList(TSDataType.FLOAT, TSDataType.FLOAT);

  private final List<Object> partialNullValues = Arrays.asList(null, 25.34F);
  private final List<Object> allNullValues = Arrays.asList(null, null);

  private Tablet partialNullTablet;
  private Tablet allNullTablet;

  private void constructTablet() {
    final MeasurementSchema[] schemas = new MeasurementSchema[2];
    for (int i = 0; i < schemas.length; i++) {
      schemas[i] = new MeasurementSchema(measurements.get(i), types.get(i));
    }

    final BitMap[] bitMapsForPartialNull = new BitMap[2];
    bitMapsForPartialNull[0] = new BitMap(1);
    bitMapsForPartialNull[0].markAll();
    bitMapsForPartialNull[1] = new BitMap(1);

    final BitMap[] bitMapsForAllNull = new BitMap[2];
    bitMapsForAllNull[0] = new BitMap(1);
    bitMapsForAllNull[0].markAll();
    bitMapsForAllNull[1] = new BitMap(1);
    bitMapsForAllNull[1].markAll();

    final Object[] valuesForPartialNull = new Object[2];
    valuesForPartialNull[0] = new float[] {0F};
    valuesForPartialNull[1] = new float[] {25.34F};

    final Object[] valuesForAllNull = new Object[2];
    valuesForAllNull[0] = new float[] {0F};
    valuesForAllNull[1] = new float[] {0F};

    partialNullTablet = new Tablet(deviceId, Arrays.asList(schemas), 1);
    partialNullTablet.values = valuesForPartialNull;
    partialNullTablet.timestamps = new long[] {3};
    partialNullTablet.rowSize = 1;
    partialNullTablet.bitMaps = bitMapsForPartialNull;

    allNullTablet = new Tablet(deviceId, Arrays.asList(schemas), 1);
    allNullTablet.values = valuesForAllNull;
    allNullTablet.timestamps = new long[] {4};
    allNullTablet.rowSize = 1;
    allNullTablet.bitMaps = bitMapsForAllNull;
  }

  @Override
  @Before
  public void setUp() throws PipeEnvironmentException {
    super.setUp();

    constructTablet();

    // init INSERT_NULL_VALUE_MAP
    INSERT_NULL_VALUE_MAP.put(
        InsertType.SESSION_INSERT_RECORD,
        (isAligned) -> {
          try {
            try (ISession session = senderEnv.getSessionConnection()) {
              if (isAligned) {
                session.insertAlignedRecord(deviceId, 3, measurements, types, partialNullValues);
                session.insertAlignedRecord(deviceId, 4, measurements, types, allNullValues);
              } else {
                session.insertRecord(deviceId, 3, measurements, types, partialNullValues);
                session.insertRecord(deviceId, 4, measurements, types, allNullValues);
              }
            } catch (StatementExecutionException e) {
              fail(e.getMessage());
            }
          } catch (IoTDBConnectionException e) {
            fail(e.getMessage());
          }
        });

    INSERT_NULL_VALUE_MAP.put(
        InsertType.SESSION_INSERT_TABLET,
        (isAligned) -> {
          try {
            try (ISession session = senderEnv.getSessionConnection()) {
              if (isAligned) {
                session.insertAlignedTablet(partialNullTablet);
                session.insertAlignedTablet(allNullTablet);
              } else {
                session.insertTablet(partialNullTablet);
                session.insertTablet(allNullTablet);
              }
            } catch (StatementExecutionException e) {
              fail(e.getMessage());
            }
          } catch (IoTDBConnectionException e) {
            fail(e.getMessage());
          }
        });

    INSERT_NULL_VALUE_MAP.put(
        InsertType.SQL_INSERT,
        (isAligned) -> {
          // partial null
          if (!TestUtils.tryExecuteNonQueriesWithRetry(
              senderEnv,
              isAligned
                  ? Collections.singletonList(
                      "insert into root.sg.d1(time, s0, s1) aligned values (3, null, 25.34)")
                  : Collections.singletonList(
                      "insert into root.sg.d1(time, s0, s1) values (3, null, 25.34)"))) {
            fail();
          }
          // all null
          if (!TestUtils.tryExecuteNonQueriesWithRetry(
              senderEnv,
              isAligned
                  ? Collections.singletonList(
                      "insert into root.sg.d1(time, s0, s1) aligned values (4, null, null)")
                  : Collections.singletonList(
                      "insert into root.sg.d1(time, s0, s1) values (4, null, null)"))) {
            fail();
          }
        });
  }

  private void testInsertNullValueTemplate(
      InsertType insertType, boolean isAligned, boolean withParsing) throws Exception {
    DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);

    String receiverIp = receiverDataNode.getIp();
    int receiverPort = receiverDataNode.getPort();

    try (SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {
      Map<String, String> extractorAttributes = new HashMap<>();
      Map<String, String> processorAttributes = new HashMap<>();
      Map<String, String> connectorAttributes = new HashMap<>();

      connectorAttributes.put("connector", "iotdb-thrift-connector");
      connectorAttributes.put("connector.ip", receiverIp);
      connectorAttributes.put("connector.port", Integer.toString(receiverPort));

      if (withParsing) {
        extractorAttributes.put("extractor.pattern", "root.sg.d1");
      }

      TSStatus status =
          client.createPipe(
              new TCreatePipeReq("test", connectorAttributes)
                  .setExtractorAttributes(extractorAttributes)
                  .setProcessorAttributes(processorAttributes));
      Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
    }

    if (!TestUtils.tryExecuteNonQueriesWithRetry(
        receiverEnv, isAligned ? CREATE_ALIGNED_TIMESERIES_SQL : CREATE_TIMESERIES_SQL)) {
      fail();
    }

    if (!TestUtils.tryExecuteNonQueriesWithRetry(
        senderEnv, isAligned ? CREATE_ALIGNED_TIMESERIES_SQL : CREATE_TIMESERIES_SQL)) {
      fail();
    }

    INSERT_NULL_VALUE_MAP.get(insertType).accept(isAligned);

    TestUtils.assertDataOnEnv(
        receiverEnv,
        "select count(*) from root.**",
        "count(root.sg.d1.s0),count(root.sg.d1.s1),",
        Collections.singleton("0,1,"));
  }

  // ---------------------- //
  // Scenario 1: SQL Insert //
  // ---------------------- //
  @Test
  public void testSQLInsertWithParsing() throws Exception {
    testInsertNullValueTemplate(InsertType.SQL_INSERT, false, true);
  }

  @Test
  public void testSQLInsertWithoutParsing() throws Exception {
    testInsertNullValueTemplate(InsertType.SQL_INSERT, false, false);
  }

  @Test
  public void testSQLInsertAlignedWithParsing() throws Exception {
    testInsertNullValueTemplate(InsertType.SQL_INSERT, true, true);
  }

  @Test
  public void testSQLInsertAlignedWithoutParsing() throws Exception {
    testInsertNullValueTemplate(InsertType.SQL_INSERT, true, false);
  }

  // --------------------------------- //
  // Scenario 2: Session Insert Record //
  // --------------------------------- //
  @Test
  public void testSessionInsertRecordWithParsing() throws Exception {
    testInsertNullValueTemplate(InsertType.SESSION_INSERT_RECORD, false, true);
  }

  @Test
  public void testSessionInsertRecordWithoutParsing() throws Exception {
    testInsertNullValueTemplate(InsertType.SESSION_INSERT_RECORD, false, false);
  }

  @Test
  public void testSessionInsertRecordAlignedWithParsing() throws Exception {
    testInsertNullValueTemplate(InsertType.SESSION_INSERT_RECORD, true, true);
  }

  @Test
  public void testSessionInsertRecordAlignedWithoutParsing() throws Exception {
    testInsertNullValueTemplate(InsertType.SESSION_INSERT_RECORD, true, false);
  }

  // --------------------------------- //
  // Scenario 3: Session Insert Tablet //
  // --------------------------------- //
  @Test
  public void testSessionInsertTabletWithParsing() throws Exception {
    testInsertNullValueTemplate(InsertType.SESSION_INSERT_TABLET, false, true);
  }

  @Test
  public void testSessionInsertTabletWithoutParsing() throws Exception {
    testInsertNullValueTemplate(InsertType.SESSION_INSERT_TABLET, false, false);
  }

  @Test
  public void testSessionInsertTabletAlignedWithParsing() throws Exception {
    testInsertNullValueTemplate(InsertType.SESSION_INSERT_TABLET, true, true);
  }

  @Test
  public void testSessionInsertTabletAlignedWithoutParsing() throws Exception {
    testInsertNullValueTemplate(InsertType.SESSION_INSERT_TABLET, true, false);
  }
}
