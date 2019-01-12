package org.apache.iotdb.db.exception.qp;

/**
 * This exception is thrown while meeting error in optimizing logical operator
 */
public class LogicalOptimizeException extends LogicalOperatorException {

    private static final long serialVersionUID = -7098092782689670064L;

    public LogicalOptimizeException(String msg) {
        super(msg);
    }

}
