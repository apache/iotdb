package cn.edu.tsinghua.iotdb.exception.codebased;

import cn.edu.tsinghua.iotdb.exception.builder.ExceptionBuilder;

public class ConnectionHostException extends IoTDBException {
    public ConnectionHostException() {
        super(ExceptionBuilder.CONN_HOST_ERROR);
    }
    public ConnectionHostException(String ip, String port, String additionalInfo) {
        super(ExceptionBuilder.CONN_HOST_ERROR, additionalInfo);
        defaultInfo=String.format(defaultInfo, ip, port);
    }
}
