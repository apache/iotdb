package cn.edu.tsinghua.iotdb.exception.codebased;

import cn.edu.tsinghua.iotdb.exception.builder.ExceptionBuilder;

public class InvalidParameterException extends IoTDBException{
    public InvalidParameterException() {
        super(ExceptionBuilder.INVALID﻿_PARAMETER_NO);
    }
    public InvalidParameterException(String additionalInfo) {
        super(ExceptionBuilder.INVALID﻿_PARAMETER_NO, additionalInfo);
    }
}
