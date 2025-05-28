package org.apache.iotdb.db.exception.runtime;

import org.apache.iotdb.commons.exception.IoTDBRuntimeException;
import org.apache.iotdb.rpc.TSStatusCode;

public class TableNotExistsRuntimeException extends IoTDBRuntimeException {

  public TableNotExistsRuntimeException(final String databaseName, final String tableName) {
    super(
        String.format("Table %s in the database %s is not exists.", tableName, databaseName),
        TSStatusCode.TABLE_NOT_EXISTS.getStatusCode());
  }

  public TableNotExistsRuntimeException(final Throwable cause) {
    super(cause, TSStatusCode.TABLE_NOT_EXISTS.getStatusCode());
  }
}
