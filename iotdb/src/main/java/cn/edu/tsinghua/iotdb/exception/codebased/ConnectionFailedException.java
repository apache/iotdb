package cn.edu.tsinghua.iotdb.exception.codebased;

import cn.edu.tsinghua.iotdb.exception.builder.ExceptionBuilder;

public class ConnectionFailedException extends IoTDBException {
    public ConnectionFailedException() {
        super(ExceptionBuilder.CON_FAIL_ERR);
    }
    public ConnectionFailedException(String additionalInfo) {
        super(ExceptionBuilder.CON_FAIL_ERR, additionalInfo);
    }
}
