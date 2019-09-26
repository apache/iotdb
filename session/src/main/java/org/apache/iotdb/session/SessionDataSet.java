package org.apache.iotdb.session;

import java.sql.SQLException;
import java.util.Iterator;
import java.util.List;
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

/**
 * Created by SilverNarcissus on 2019/9/26.
 */
public class SessionDataSet {

  private boolean getFlag = false;
  private String sql;
  private long queryId;
  private RowRecord record;
  private Iterator<RowRecord> recordItr;
  private TSIService.Iface client = null;
  private TSOperationHandle operationHandle;


  public SessionDataSet(String sql, long queryId, TSIService.Iface client,
      TSOperationHandle operationHandle) {
    this.sql = sql;
    this.queryId = queryId;
    this.client = client;
    this.operationHandle = operationHandle;
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
      TSFetchResultsReq req = new TSFetchResultsReq(sql, 512, queryId);

      try {
        TSFetchResultsResp resp = client.fetchResults(req);

        RpcUtils.verifySuccess(resp.getStatus());

        if (!resp.hasResultSet) {
          return false;
        } else {
          TSQueryDataSet tsQueryDataSet = resp.getQueryDataSet();
          List<RowRecord> records = SessionUtils.convertRowRecords(tsQueryDataSet);
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
      throw new SQLException("Error occurs for close opeation in server side. The reason is " + e);
    } catch (TException e) {
      throw new SQLException(
          "Error occurs when connecting to server for close operation, because: " + e);
    }
  }
}
