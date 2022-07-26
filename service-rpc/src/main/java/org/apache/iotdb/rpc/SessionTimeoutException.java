package org.apache.iotdb.rpc;

import org.apache.iotdb.service.rpc.thrift.TSStatus;

public class SessionTimeoutException extends StatementExecutionException {
  public SessionTimeoutException(TSStatus status) {
    super(status);
  }

  public SessionTimeoutException(org.apache.iotdb.protocol.influxdb.rpc.thrift.TSStatus status) {
    super(status);
  }

  public SessionTimeoutException(String reason) {
    super(reason);
  }

  public SessionTimeoutException(Throwable cause) {
    super(cause);
  }

  public SessionTimeoutException(String message, Throwable cause) {
    super(message, cause);
  }
}
