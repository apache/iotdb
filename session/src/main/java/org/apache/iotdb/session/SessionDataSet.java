/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb.session;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import org.apache.iotdb.rpc.IoTDBRPCException;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.service.rpc.thrift.TSCloseOperationReq;
import org.apache.iotdb.service.rpc.thrift.TSFetchResultsReq;
import org.apache.iotdb.service.rpc.thrift.TSFetchResultsResp;
import org.apache.iotdb.service.rpc.thrift.TSIService;
import org.apache.iotdb.service.rpc.thrift.TSOperationHandle;
import org.apache.iotdb.service.rpc.thrift.TSQueryDataSet;
import org.apache.iotdb.service.rpc.thrift.TSStatus;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.thrift.TException;

public class SessionDataSet {

  private boolean getFlag = false;
  private String sql;
  private long queryId;
  private RowRecord record;
  private Iterator<RowRecord> recordItr;
  private TSIService.Iface client = null;
  private TSOperationHandle operationHandle;
  private int batchSize = 512;
  private List<String> columnTypeDeduplicatedList;
  private List<String> columnNames;

  public SessionDataSet(String sql, List<String> columnNameList, List<String> columnTypeList,
      long queryId, TSIService.Iface client, TSOperationHandle operationHandle) {
    this.sql = sql;
    this.queryId = queryId;
    this.client = client;
    this.operationHandle = operationHandle;
    this.columnNames = columnNameList;
    // deduplicate columnTypeList according to columnNameList
    this.columnTypeDeduplicatedList = new ArrayList<>();
    Set<String> columnSet = new HashSet<>(); // for deduplication
    for (int i = 0; i < columnNameList.size(); i++) {
      String name = columnNameList.get(i);
      if (!columnSet.contains(name)) {
        columnSet.add(name);
        columnTypeDeduplicatedList.add(columnTypeList.get(i));
      }
    }
  }

  public List<String> getColumnNames() {
    return columnNames;
  }

  public int getBatchSize() {
    return batchSize;
  }

  public void setBatchSize(int batchSize) {
    this.batchSize = batchSize;
  }

  public boolean hasNext() throws SQLException, IoTDBRPCException {
    return getFlag || nextWithoutConstraints(sql, queryId);
  }

  public RowRecord next() throws SQLException, IoTDBRPCException {
    if (!getFlag) {
      nextWithoutConstraints(sql, queryId);
    }

    getFlag = false;
    return record;
  }


  private boolean nextWithoutConstraints(String sql, long queryId)
      throws SQLException, IoTDBRPCException {
    if ((recordItr == null || !recordItr.hasNext())) {
      TSFetchResultsReq req = new TSFetchResultsReq(sql, batchSize, queryId);

      try {
        TSFetchResultsResp resp = client.fetchResults(req);

        RpcUtils.verifySuccess(resp.getStatus());

        if (!resp.hasResultSet) {
          return false;
        } else {
          TSQueryDataSet tsQueryDataSet = resp.getQueryDataSet();
          List<RowRecord> records = SessionUtils
              .convertRowRecords(tsQueryDataSet, columnTypeDeduplicatedList);
          recordItr = records.iterator();
        }
      } catch (TException e) {
        throw new SQLException(
            "Cannot fetch result from server, because of network connection : {} ", e);
      }

    }

    record = recordItr.next();
    getFlag = true;
    return true;
  }

  public void closeOperationHandle() throws SQLException {
    try {
      if (operationHandle != null) {
        TSCloseOperationReq closeReq = new TSCloseOperationReq(operationHandle, queryId);
        TSStatus closeResp = client.closeOperation(closeReq);
        RpcUtils.verifySuccess(closeResp);
      }
    } catch (IoTDBRPCException e) {
      throw new SQLException("Error occurs for close opeation in server side. The reason is " + e, e);
    } catch (TException e) {
      throw new SQLException(
          "Error occurs when connecting to server for close operation, because: " + e, e);
    }
  }
}
