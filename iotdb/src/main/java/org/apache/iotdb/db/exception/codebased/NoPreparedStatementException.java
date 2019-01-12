package org.apache.iotdb.db.exception.codebased;

import org.apache.iotdb.db.exception.builder.ExceptionBuilder;

public class NoPreparedStatementException extends IoTDBException {
    public NoPreparedStatementException() {
        super(ExceptionBuilder.NO_PREPARE_STMT);
    }
    public NoPreparedStatementException(String additionalInfo) {
        super(ExceptionBuilder.NO_PREPARE_STMT, additionalInfo);
    }
}
