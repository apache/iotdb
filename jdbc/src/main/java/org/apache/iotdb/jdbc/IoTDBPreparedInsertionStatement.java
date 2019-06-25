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

  private long timestamp;
  private String deviceId;
  private List<String> measurements;
  private List<String> values;
  private TSInsertionReq req = new TSInsertionReq();

  public IoTDBPreparedInsertionStatement(IoTDBConnection connection,
      Iface client,
      TS_SessionHandle sessionHandle, ZoneId zoneId) {
    super(connection, client, sessionHandle, zoneId);
  }

  @Override
  public boolean execute() throws SQLException {
    req.setDeviceId(deviceId);
    req.setMeasurements(measurements);
    req.setValues(values);
    req.setTimestamp(timestamp);
    try {
      TSExecuteStatementResp resp = client.executeInsertion(req);
      return resp.getStatus().getStatusCode() == TS_StatusCode.SUCCESS_STATUS;
    } catch (TException e) {
      throw new SQLException(e);
    }
  }

  public void setTimestamp(long timestamp) {
    this.timestamp = timestamp;
  }

  public void setDeviceId(String deviceId) {
    this.deviceId = deviceId;
  }

  public void setMeasurements(List<String> measurements) {
    this.measurements = measurements;
  }

  public void setValues(List<String> values) {
    this.values = values;
  }

}
