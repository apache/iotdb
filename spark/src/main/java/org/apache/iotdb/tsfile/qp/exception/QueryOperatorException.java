package org.apache.iotdb.tsfile.qp.exception;

public class QueryOperatorException extends LogicalOptimizeException {

    private static final long serialVersionUID = 8581594261924961899L;

    public QueryOperatorException(String msg) {
        super(msg);
    }

}
