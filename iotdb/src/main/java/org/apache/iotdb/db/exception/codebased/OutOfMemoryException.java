package org.apache.iotdb.db.exception.codebased;

import org.apache.iotdb.db.exception.builder.ExceptionBuilder;

public class OutOfMemoryException extends  IoTDBException{
    public OutOfMemoryException() {
        super(ExceptionBuilder.OUT_OF_MEMORY);
    }
    public OutOfMemoryException(String additionalInfo) {
        super(ExceptionBuilder.OUT_OF_MEMORY, additionalInfo);
    }
}
