package org.apache.iotdb.db.exception.codebased;

import org.apache.iotdb.db.exception.builder.ExceptionBuilder;

public class ConnectionHostException extends IoTDBException {
    public ConnectionHostException() {
        super(ExceptionBuilder.CONN_HOST_ERROR);
    }
    public ConnectionHostException(String ip, String port, String additionalInfo) {
        super(ExceptionBuilder.CONN_HOST_ERROR, additionalInfo);
        defaultInfo=String.format(defaultInfo, ip, port);
    }
}
