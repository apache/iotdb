package org.apache.iotdb.db.exception.runtime;

import org.apache.iotdb.commons.exception.IoTDBRuntimeException;
import org.apache.iotdb.rpc.TSStatusCode;

public class TableLostRuntimeException extends IoTDBRuntimeException {

  public TableLostRuntimeException(final String databaseName, final String tableName) {
    super(
        String.format("Table %s in the database %s is lost unexpected.", tableName, databaseName),
        TSStatusCode.TABLE_IS_LOST.getStatusCode());
  }

  public TableLostRuntimeException(final Throwable cause) {
    super(cause, TSStatusCode.TABLE_IS_LOST.getStatusCode());
  }
}
