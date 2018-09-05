package cn.edu.tsinghua.iotdb.exception.codebased;

import cn.edu.tsinghua.iotdb.exception.builder.ExceptionBuilder;

public class AuthPluginException extends IoTDBException {
    public AuthPluginException() {
        super(ExceptionBuilder.AUTH_PLUGIN_ERR);
    }
    public AuthPluginException(String userName, String additionalInfo) {
        super(ExceptionBuilder.AUTH_PLUGIN_ERR, additionalInfo);
        defaultInfo = String.format(defaultInfo, userName);
    }
}

