package org.apache.iotdb.db.exception.query;

import org.apache.iotdb.commons.exception.IoTDBException;
import org.apache.iotdb.rpc.TSStatusCode;

public class KilledByOthersException extends IoTDBException {
  private static final long serialVersionUID = -6027957067833327712L;

  public static final String MESSAGE = "Query was killed by others";

  public KilledByOthersException() {
    super(MESSAGE, TSStatusCode.QUERY_WAS_KILLED.getStatusCode(), true);
  }
}
