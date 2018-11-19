package cn.edu.tsinghua.iotdb.exception.codebased;

import cn.edu.tsinghua.iotdb.exception.builder.ExceptionBuilder;

public class OutOfMemoryException extends  IoTDBException{
    public OutOfMemoryException() {
        super(ExceptionBuilder.OUT_OF_MEMORY);
    }
    public OutOfMemoryException(String additionalInfo) {
        super(ExceptionBuilder.OUT_OF_MEMORY, additionalInfo);
    }
}
