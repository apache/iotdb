package cn.edu.tsinghua.iotdb.exception.codebased;

import cn.edu.tsinghua.iotdb.exception.builder.ExceptionBuilder;

public class NoParameterException extends IoTDBException{
    public NoParameterException() {
        super(ExceptionBuilder.NO_PARAMETERS_EXISTS);
    }
    public NoParameterException(String additionalInfo) {
        super(ExceptionBuilder.NO_PARAMETERS_EXISTS, additionalInfo);
    }
}
