package org.apache.iotdb.jdbc;

import java.sql.SQLException;
import java.time.ZoneId;
import java.util.List;
import org.apache.iotdb.service.rpc.thrift.TSExecuteStatementResp;
import org.apache.iotdb.service.rpc.thrift.TSIService.Iface;
import org.apache.iotdb.service.rpc.thrift.TSInsertionReq;
import org.apache.iotdb.service.rpc.thrift.TS_SessionHandle;
import org.apache.iotdb.service.rpc.thrift.TS_StatusCode;
import org.apache.thrift.TException;

public class IoTDBPreparedInsertionStatement extends IoTDBPreparedStatement {

  private TSInsertionReq req = new TSInsertionReq();

  public IoTDBPreparedInsertionStatement(IoTDBConnection connection,
      Iface client,
      TS_SessionHandle sessionHandle, ZoneId zoneId) throws SQLException {
    super(connection, client, sessionHandle, zoneId);
    req.setStmtId(stmtId);
  }

  @Override
  public boolean execute() throws SQLException {

    try {
      TSExecuteStatementResp resp = client.executeInsertion(req);
      req.unsetDeviceId();
      req.unsetMeasurements();
      req.unsetTimestamp();
      req.unsetValues();
      return resp.getStatus().getStatusCode() == TS_StatusCode.SUCCESS_STATUS;
    } catch (TException e) {
      throw new SQLException(e);
    }
  }

  public void setTimestamp(long timestamp) {
    req.setTimestamp(timestamp);
  }

  public void setDeviceId(String deviceId) {
    req.setDeviceId(deviceId);
  }

  public void setMeasurements(List<String> measurements) {
    req.setMeasurements(measurements);
  }

  public void setValues(List<String> values) {
    req.setValues(values);
  }
}
