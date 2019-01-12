package org.apache.iotdb.db.exception.codebased;

import org.apache.iotdb.db.exception.builder.ExceptionBuilder;

public class NoParameterException extends IoTDBException{
    public NoParameterException() {
        super(ExceptionBuilder.NO_PARAMETERS_EXISTS);
    }
    public NoParameterException(String additionalInfo) {
        super(ExceptionBuilder.NO_PARAMETERS_EXISTS, additionalInfo);
    }
}
