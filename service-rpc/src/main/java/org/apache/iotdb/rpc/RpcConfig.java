package org.apache.iotdb.rpc;


/**
 * @author chenPeng
 * @version 1.0.0
 * @ClassName Config.java
 * @Description TODO
 * @createTime 2020年08月19日 22:26:00
 */
public class RpcConfig {
    public enum Constant {
        NUMBER, BOOLEAN
    }

    private RpcConfig(){}

    static Constant boolFormat = Constant.BOOLEAN;
    public static boolean rpcThriftCompressionEnable = false;
    public static int connectionTimeoutInMs = 0;
    public static final int RETRY_NUM = 3;
    public static final long RETRY_INTERVAL = 1000;
    public static int fetchSize = 10000;

    public static void setBoolFormat(Constant boolFormat) {
        RpcConfig.boolFormat = boolFormat;
    }

}
