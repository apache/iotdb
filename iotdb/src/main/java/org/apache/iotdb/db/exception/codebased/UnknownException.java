package org.apache.iotdb.db.exception.codebased;

import org.apache.iotdb.db.exception.builder.ExceptionBuilder;

public class UnknownException extends  IoTDBException {
    public UnknownException() {
        super(ExceptionBuilder.UNKNOWN_ERROR);
    }
    public UnknownException(String additionalInfo) {
        super(ExceptionBuilder.UNKNOWN_ERROR, additionalInfo);
    }
}
