package org.apache.iotdb.db.exception.codebased;

import org.apache.iotdb.db.exception.builder.ExceptionBuilder;

public class ConnectionFailedException extends IoTDBException {
    public ConnectionFailedException() {
        super(ExceptionBuilder.CON_FAIL_ERR);
    }
    public ConnectionFailedException(String additionalInfo) {
        super(ExceptionBuilder.CON_FAIL_ERR, additionalInfo);
    }
}
