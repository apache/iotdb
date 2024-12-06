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
import org.apache.iotdb.db.it.utils.TestUtils;
import org.apache.iotdb.it.env.cluster.node.DataNodeWrapper;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.MultiClusterIT2TableModel;
import org.apache.iotdb.rpc.TSStatusCode;

import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

@RunWith(IoTDBTestRunner.class)
@Category({MultiClusterIT2TableModel.class})
public class IoTDBPipeNullValueIT extends AbstractPipeTableModelTestIT {

  private enum InsertType {
    SESSION_INSERT_RECORD,
    SESSION_INSERT_TABLET,
    SQL_INSERT,
  }

  private void testInsertNullValueTemplate(
      final InsertType insertType, final boolean withParsing, final String realtime)
      throws Exception {
    final DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);

    final String receiverIp = receiverDataNode.getIp();
    final int receiverPort = receiverDataNode.getPort();
    final Consumer<String> handleFailure =
        o -> {
          TestUtils.executeNonQueryWithRetry(senderEnv, "flush");
          TestUtils.executeNonQueryWithRetry(receiverEnv, "flush");
        };

    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {
      final Map<String, String> extractorAttributes = new HashMap<>();
      final Map<String, String> processorAttributes = new HashMap<>();
      final Map<String, String> connectorAttributes = new HashMap<>();

      TableModelUtils.createDataBaseAndTable(senderEnv, "test", "test");

      if (insertType == InsertType.SESSION_INSERT_TABLET) {
        TableModelUtils.insertDataByTablet("test", "test", 0, 200, senderEnv, true);
      } else if (insertType == InsertType.SQL_INSERT) {
        TableModelUtils.insertData("test", "test", 0, 200, senderEnv, true);
      }

      connectorAttributes.put("connector", "iotdb-thrift-connector");
      connectorAttributes.put("connector.ip", receiverIp);
      connectorAttributes.put("connector.port", Integer.toString(receiverPort));

      extractorAttributes.put("capture.table", "true");
      extractorAttributes.put("realtime-mode", realtime);
      if (withParsing) {
        extractorAttributes.put("start-time", "150");
        extractorAttributes.put("end-time", "249");
        extractorAttributes.put("database-name", "test");
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
      TableModelUtils.insertDataByTablet("test", "test", 200, 400, senderEnv, true);
    } else if (insertType == InsertType.SQL_INSERT) {
      TableModelUtils.insertData("test", "test", 200, 400, senderEnv, true);
    }

    if (withParsing) {
      TableModelUtils.assertCountData("test", "test", 100, receiverEnv, handleFailure);
      return;
    }
    TableModelUtils.assertCountData("test", "test", 400, receiverEnv, handleFailure);
  }

  // ---------------------- //
  // Scenario 1: SQL Insert //
  // ---------------------- //
  @Test
  public void testSQLInsertWithParsingForcedLog() throws Exception {
    testInsertNullValueTemplate(InsertType.SQL_INSERT, true, "forced-log");
  }

  @Test
  public void testSQLInsertWithoutParsingForcedLog() throws Exception {
    testInsertNullValueTemplate(InsertType.SQL_INSERT, false, "forced-log");
  }

  @Test
  public void testSQLInsertWithParsingFile() throws Exception {
    testInsertNullValueTemplate(InsertType.SQL_INSERT, true, "file");
  }

  @Test
  public void testSQLInsertWithoutParsingFile() throws Exception {
    testInsertNullValueTemplate(InsertType.SQL_INSERT, false, "file");
  }

  @Test
  public void testSQLInsertWithParsingStream() throws Exception {
    testInsertNullValueTemplate(InsertType.SQL_INSERT, true, "stream");
  }

  @Test
  public void testSQLInsertWithoutParsingStream() throws Exception {
    testInsertNullValueTemplate(InsertType.SQL_INSERT, false, "stream");
  }

  // --------------------------------- //
  // Scenario 2: Session Insert Tablet //
  // --------------------------------- //
  @Test
  public void testSessionInsertTabletWithParsingForcedLog() throws Exception {
    testInsertNullValueTemplate(InsertType.SESSION_INSERT_TABLET, true, "forced-log");
  }

  @Test
  public void testSessionInsertTabletWithoutParsingForcedLog() throws Exception {
    testInsertNullValueTemplate(InsertType.SESSION_INSERT_TABLET, false, "forced-log");
  }

  @Test
  public void testSessionInsertTabletWithParsingFile() throws Exception {
    testInsertNullValueTemplate(InsertType.SESSION_INSERT_TABLET, true, "file");
  }

  @Test
  public void testSessionInsertTabletWithoutParsingFile() throws Exception {
    testInsertNullValueTemplate(InsertType.SESSION_INSERT_TABLET, false, "file");
  }

  @Test
  public void testSessionInsertTabletWithParsingStream() throws Exception {
    testInsertNullValueTemplate(InsertType.SESSION_INSERT_TABLET, true, "stream");
  }

  @Test
  public void testSessionInsertTabletWithoutParsingStream() throws Exception {
    testInsertNullValueTemplate(InsertType.SESSION_INSERT_TABLET, false, "stream");
  }
}
