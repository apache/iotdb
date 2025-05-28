package org.apache.iotdb.db.exception.runtime;

import org.apache.iotdb.commons.exception.IoTDBRuntimeException;
import org.apache.iotdb.rpc.TSStatusCode;

public class RegisterTableSchemaFailureException extends IoTDBRuntimeException {

  public RegisterTableSchemaFailureException(final String databaseName, final String tableName) {
    super(
        String.format(
            "The schema register failed that table %s in the database %s.",
            tableName, databaseName),
        TSStatusCode.TABLE_NOT_EXISTS.getStatusCode());
  }

  public RegisterTableSchemaFailureException(final Throwable cause) {
    super(cause, TSStatusCode.TABLE_NOT_EXISTS.getStatusCode());
  }
}
