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

package org.apache.iotdb.pipe.it.tablemodel;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.client.sync.SyncConfigNodeIServiceClient;
import org.apache.iotdb.confignode.rpc.thrift.TCreatePipeReq;
import org.apache.iotdb.it.env.cluster.node.DataNodeWrapper;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.MultiClusterIT2ManualCreateSchema;
import org.apache.iotdb.rpc.TSStatusCode;

import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.HashMap;
import java.util.Map;

@RunWith(IoTDBTestRunner.class)
@Category({MultiClusterIT2ManualCreateSchema.class})
public class IoTDBPipeNullValueIT extends AbstractPipeTableModelTestIT {

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

  private void testInsertNullValueTemplate(final InsertType insertType, final boolean withParsing)
      throws Exception {
    final DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);

    final String receiverIp = receiverDataNode.getIp();
    final int receiverPort = receiverDataNode.getPort();

    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {
      final Map<String, String> extractorAttributes = new HashMap<>();
      final Map<String, String> processorAttributes = new HashMap<>();
      final Map<String, String> connectorAttributes = new HashMap<>();

      Utils.createDataBaseAndTable(senderEnv, "test", "test");

      if (insertType == InsertType.SESSION_INSERT_TABLET) {
        Utils.insertDataByTablet("test", "test", 0, 50, senderEnv, true);
      } else if (insertType == InsertType.SQL_INSERT) {
        Utils.insertData("test", "test", 0, 50, senderEnv, true);
      }

      connectorAttributes.put("connector", "iotdb-thrift-connector");
      connectorAttributes.put("connector.ip", receiverIp);
      connectorAttributes.put("connector.port", Integer.toString(receiverPort));

      extractorAttributes.put("capture.table", "true");
      extractorAttributes.put("realtime-mode", "forced-log");
      if (withParsing) {
        extractorAttributes.put("start-time", "25");
        extractorAttributes.put("end-time", "74");
        extractorAttributes.put("database-name", "Test");
        extractorAttributes.put("table-name", "test");
      }

      final TSStatus status =
          client.createPipe(
              new TCreatePipeReq("test", connectorAttributes)
                  .setExtractorAttributes(extractorAttributes)
                  .setProcessorAttributes(processorAttributes));
      Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
    }

    if (insertType == InsertType.SESSION_INSERT_TABLET) {
      Utils.insertDataByTablet("test", "test", 50, 100, senderEnv, true);
    } else if (insertType == InsertType.SQL_INSERT) {
      Utils.insertData("test", "test", 50, 100, senderEnv, true);
    }

    if (withParsing) {
      Utils.assertCountData("test", "test", 50, receiverEnv);
      return;
    }
    Utils.assertCountData("test", "test", 100, receiverEnv);
  }

  // ---------------------- //
  // Scenario 1: SQL Insert //
  // ---------------------- //
  @Test
  public void testSQLInsertWithParsing() throws Exception {
    testInsertNullValueTemplate(InsertType.SQL_INSERT, true);
  }

  @Test
  public void testSQLInsertWithoutParsing() throws Exception {
    testInsertNullValueTemplate(InsertType.SQL_INSERT, false);
  }

  // --------------------------------- //
  // Scenario 2: Session Insert Tablet //
  // --------------------------------- //
  @Test
  public void testSessionInsertTabletWithParsing() throws Exception {
    testInsertNullValueTemplate(InsertType.SESSION_INSERT_TABLET, true);
  }

  @Test
  public void testSessionInsertTabletWithoutParsing() throws Exception {
    testInsertNullValueTemplate(InsertType.SESSION_INSERT_TABLET, false);
  }
}
