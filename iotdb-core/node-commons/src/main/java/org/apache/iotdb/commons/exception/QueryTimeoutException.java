package org.apache.iotdb.commons.exception;

import static org.apache.iotdb.rpc.TSStatusCode.QUERY_TIMEOUT;

public class QueryTimeoutException extends IoTDBRuntimeException {

  public QueryTimeoutException() {
    super("Query execution is time out", QUERY_TIMEOUT.getStatusCode(), true);
  }
}
