package cn.edu.tsinghua.iotdb.exception.codebased;

import cn.edu.tsinghua.iotdb.exception.builder.ExceptionBuilder;

public class NoPreparedStatementException extends IoTDBException {
    public NoPreparedStatementException() {
        super(ExceptionBuilder.NO_PREPARE_STMT);
    }
    public NoPreparedStatementException(String additionalInfo) {
        super(ExceptionBuilder.NO_PREPARE_STMT, additionalInfo);
    }
}
