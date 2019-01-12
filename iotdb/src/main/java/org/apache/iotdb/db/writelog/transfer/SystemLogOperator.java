package org.apache.iotdb.db.writelog.transfer;

/**
 *  To avoid conflict with org.apache.iotdb.tsfiledb.qp.constant.SQLConstant.Operator.
 */
public class SystemLogOperator {
   public static final int INSERT = 0;
   public static final int UPDATE = 1;
   public static final int DELETE = 2;
   public static final int OVERFLOWFLUSHSTART = 3;
   public static final int OVERFLOWFLUSHEND = 4;
   public static final int BUFFERFLUSHSTART = 5;
   public static final int BUFFERFLUSHEND = 6;
}
