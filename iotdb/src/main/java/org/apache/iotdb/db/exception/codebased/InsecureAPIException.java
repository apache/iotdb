package org.apache.iotdb.db.exception.codebased;

import org.apache.iotdb.db.exception.builder.ExceptionBuilder;

public class InsecureAPIException extends IoTDBException{
    public InsecureAPIException() {
        super(ExceptionBuilder.INSECURE_API_ERR);
    }
    public InsecureAPIException(String functionName, String additionalInfo) {
        super(ExceptionBuilder.INSECURE_API_ERR, additionalInfo);
        defaultInfo=String.format(defaultInfo, functionName);
    }
}
